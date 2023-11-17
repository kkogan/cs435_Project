import argparse
import os
import numpy as np

from urllib.parse import unquote
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, BinaryType

from pyspark.sql.functions import split, element_at, udf
from PIL import Image
from dataclasses import dataclass 
from functools import partial
from typing import List

from augmentations_config import augmentations_for_row


MANIFEST_SCHEMA = StructType([
    StructField("file_name", StringType(), True),
    StructField("hdfs_path", StringType(), True),
    StructField("species", StringType(), True),
    StructField("is_healthy", IntegerType(), True),
    StructField("class_name", StringType(), True),
    StructField("source_1", StringType(), True),
    StructField("source_2", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("augmentation", StringType(), True)
])

AUGMENTATION_SCHEMA = StructType(
    MANIFEST_SCHEMA.fields[:] + [
        StructField("new_aug", StringType(), True),
        StructField("aug_img", BinaryType(), True),
        StructField("aug_img_w", IntegerType(), True),
        StructField("aug_img_h", IntegerType(), True)
    ]
)


@dataclass
class Config:
    hdfs_input_root:str
    spark_name:str
    spark_local:int
    local_output_root:str
    manifest_input:str
    manifest_output:str
    limit:int


def main():
    config: Config = parse_args()
    spark = spark_session(config.spark_name, config.spark_local)
    try:
        augment_dataset(config, spark)
    finally:
        spark.stop()


def augment_dataset(config:Config, spark:SparkSession) -> None:
    manifest_df = get_manifest_df(config, spark)
    images_df = get_images_df(config, spark)
    augmented_df = get_augumented_df(config, manifest_df, images_df)
    save_augmented_manifest(config, augmented_df)
    save_augmented_images(manifest_df, augmented_df)

 
def get_manifest_df(config:Config, spark:SparkSession) -> DataFrame:
    manifest_df = spark.read.csv(config.manifest_input, header=True, inferSchema=True)
    print("MANIFEST")
    manifest_df.show(5, truncate=True)
    return manifest_df


def get_images_df(config:Config, spark:SparkSession) -> DataFrame:
    decode_url_udf = udf(lambda url_encoded_str: unquote(url_encoded_str),  StringType())
    images_df = spark.read.format("image").option("dropInvalid", True).option("recursiveFileLookup","true").load(config.hdfs_input_root)
    
    if config.spark_local or config.limit > 0:
        images_df = images_df.limit(config.limit or 100)
    images_df = images_df.withColumn("file_name_encoded", element_at(split(images_df["image.origin"], "/"), -1))
    images_df = images_df.withColumn("file_name", decode_url_udf(images_df["file_name_encoded"]))
    
    print("IMAGES")
    images_df.select("file_name", "image.origin","image.nChannels", "image.width", "image.height", "image.data").show(5, truncate=True)
    return images_df


def get_augumented_df(config:Config, manifest_df:DataFrame, images_df:DataFrame) -> DataFrame:
    joined_df = images_df.join(manifest_df, images_df['file_name'] == manifest_df['file_name'])
    print("JOINED")
    joined_df.show(5, truncate=True)
    
    augment_image_func = partial(augment_image_row, config.local_output_root)
    augmented_df = joined_df\
        .rdd\
        .flatMap(augment_image_func)\
        .toDF(AUGMENTATION_SCHEMA)
    augmented_df.cache()
    
    print("AUGMENTED")
    augmented_df.show(5, truncate=True)
    return augmented_df


def save_augmented_manifest(config:Config, augmented_df:DataFrame) -> None:
    manifest_columns = [field.name for field in MANIFEST_SCHEMA.fields]
    augmented_df.select(*manifest_columns)\
        .coalesce( 1 )\
        .write.mode('overwrite').csv(config.manifest_output, header=True)


def save_augmented_images(manifest_df:DataFrame, augmented_df:DataFrame) -> None:
    # chunk up the saving to avoid overwhelming driver. Yes this is happening on local 😭😭😭
    classes = [row.class_name for row in manifest_df.select("class_name").distinct().collect()]
    for class_name in classes:
        filtered_df = augmented_df.filter((augmented_df["class_name"] == class_name) & (augmented_df["aug_img"].isNotNull()))
        for row in filtered_df.collect():
            save_image_pil(row.aug_img, row.aug_img_w, row.aug_img_h, row.hdfs_path)


def augment_image_row(hdfs_output_path, row) -> List[Row]:
    old_row = Row(**{
        **{k:v for k,v in row.asDict().items() if k not in ['image', 'file_name_encoded']},
        **dict(new_aug=None, aug_img=None, aug_img_w=None, aug_img_h=None,)
    })
    augmentations = augmentations_for_row(row)
    if augmentations is None:
        return [old_row]
    
    image = load_image_pil(row)
    
    augmented_rows = []
    for aug_name, augment in augmentations.items():
        augmented = augment(image=image)
        # some augments have a proba config, so may not augment image
        if np.array_equal(augmented['image'],image):
            continue
                
        old_aug = row.augmentation
        augmentation_suffix = f"+{aug_name}" if old_aug else f"_{aug_name}"
        base, ext = os.path.splitext(row.file_name)
        new_file_name = f"{base}{augmentation_suffix}{ext}"
        new_hdfs_path = os.path.join(hdfs_output_path, f"{row.class_name}/{new_file_name}")
        updated_augmentation_col = f"{old_aug}+{aug_name}" if old_aug else aug_name
        
        new_manifest_row = Row(**{
            **old_row.asDict(),
            **dict(
                hdfs_path=new_hdfs_path,
                file_name=new_file_name,
                augmentation=updated_augmentation_col, 
                new_aug=aug_name,
                # not sure how to put this in a nice nested struct like spark's initial image load
                aug_img=augmented['image'].tobytes(),
                aug_img_h=augmented['image'].shape[0],
                aug_img_w=augmented['image'].shape[1],
            )
        })
        augmented_rows.append(new_manifest_row)
    
    # if all augmentations were skipped due to proba config, return old row
    if not augmented_rows:
        return [old_row]
    return augmented_rows


def load_image_pil(row) -> np.ndarray:
    binary_image = row.image.data
    mode = 'RGBA' if (row.image.nChannels == 4) else 'RGB' 
    image = Image.frombytes(mode=mode, data=bytes(binary_image), size=[row.image.width, row.image.height])
    if image is None or image.size == (0,0):
        raise RuntimeError(f"Failed to load image at {row.image.origin}")
    return np.array(image)

    
def save_image_pil(aug_img, w:int, h:int, local_path:str):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    image = Image.frombytes(mode='RGB', data=bytes(aug_img), size=[w, h])
    image.save(local_path)


# def load_image_cv2(row):
#     binary_image = row.image.data
#     image_array = np.asarray(bytearray(binary_image), dtype="uint8")
#     image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
#     if image is None or image.size == 0:
#         raise RuntimeError(f"Failed to load image at {row.image.origin}")
#     return image


# def visualize_cv2(image):
#     image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
#     plt.figure(figsize=(10, 10))
#     plt.axis('off')
#     plt.imshow(image_rgb) # not working
#     plt.show()
    

def parse_args() -> Config:
    parser = argparse.ArgumentParser(description='Create a manifest table for the unpartitioned dataset.')
    
    parser.add_argument(
        '--local', 
        type=int, 
        help='Spark App name', 
        default=0)
    parser.add_argument(
        '--hdfs_input_root', 
        type=str,
        required=True,
        help='The path to the root of the input dataset in HDFS')
    parser.add_argument(
        '--local_output_root', 
        type=str, 
        required=True,
        help='Output the images to this LOCAL root. WOMP WOMP. Can\'t figure out how to HDFS this part.')
    parser.add_argument(
        '--manifest_input', 
        type=str, 
        required=True,
        help='Path to the manifest file of the input dataset')
    parser.add_argument(
        '--manifest_output', 
        type=str, 
        required=True,
        help='Path to the new manifest file for the output dataset')
    parser.add_argument(
        '--limit', 
        type=int, 
        required=False,
        help='Limit input image number for testing',
        default=0)
    parser.add_argument(
        '--spark_name', 
        type=str, 
        help='Spark App name', 
        default="augment")
     
    args = parser.parse_args()
    return Config(
        spark_name=args.spark_name,
        spark_local=args.local,
        hdfs_input_root=args.hdfs_input_root,
        local_output_root=args.local_output_root,
        manifest_input=args.manifest_input,
        manifest_output=args.manifest_output,
        limit=args.limit
    )


def spark_session(app_name:str = "default", local=False) -> SparkSession:
    if local:    
        return SparkSession.builder \
            .master("local") \
            .appName(app_name) \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()


if __name__ == "__main__": 
    main()