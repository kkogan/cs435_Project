import argparse
import subprocess
import pandas as pd
import re

from dataclasses import dataclass 
from typing import List, Dict, Any, Optional


# R_HL 9885 copy.JPG
# JR_HL 9478.JPG
# JR_HL 4097_flipTB.JPG
# GHLB2ES Leaf 69.1.JPG
# CG1.JPG
IMAGE_INFO_PATTERN = r"(\S+)(?: (.*?)(?:[_](.+))?)?\.JPG"


def main():
    config = parse_args()
    manifest = create_manifest(config.hdfs_path)
    manifest.to_csv(config.output_path, index=False)


def create_manifest(hdfs_path:str):
    filepaths = get_filepaths(hdfs_path)    
    manifest_df = parse_filepaths(filepaths)
    
    print("\n🟢 Parsed examples per class:\n", manifest_df.class_name.value_counts())
    for _, row in manifest_df.groupby('class_name').first().iterrows():
        print(row.T, '\n')
    
    return manifest_df



def get_filepaths(hdfs_path:str) -> List[str]:
    cmd = f"hdfs dfs -find {hdfs_path} -name *.[jJ][pP][gG]"
    try:
        print(cmd)
        result = subprocess.run(cmd.split(), check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Command [{cmd}] failed with exit code: {e.returncode}")
        print("Error output:", e.stderr)
        raise e
    # print("result.stdout\n", result.stdout)
    return result.stdout.strip().split('\n')


def parse_filepaths(filepaths:List[str]) -> pd.DataFrame:
    samples_metadata = []
    for i, file in enumerate(filepaths):
        parts = file.split('/')
        try:
            class_name = parts[-2]
            file_name = parts[-1]
        except IndexError:
            print(f'error for file {i}:[{file}].', f"Prev: {filepaths[i-1] if i>0 else None}",  f"Next: {filepaths[i+1] if i+1<len(filepaths) else None}")
            raise
        class_metadata = parse_class_name(class_name)
        image_metadata = parse_file_name(file_name, class_name)
        
        samples_metadata.append(
            dict(
                **dict(
                    hdfs_path=file,
                    file_name=file_name
                ),
                 **class_metadata,
                 **image_metadata
            )
        )
    return pd.DataFrame(samples_metadata)


def parse_class_name(class_name) -> Dict:
    class_parts = class_name.split('___')
    species = class_parts[0]
    is_healthy = int(class_parts[1] == 'healthy')
    return dict(
        species=species,
        is_healthy=is_healthy,
        class_name=class_name,
    )


def parse_file_name(image_name, class_name) -> Dict:
    image_parts = image_name.split('___')
    source_part = image_parts[0].strip()
    metadata_part = list_get(image_parts, 1, default=source_part)
    match = re.search(IMAGE_INFO_PATTERN, metadata_part, re.IGNORECASE)
    try:
        tag = match[1].strip() # not sure what this name component actually means
        source_2 = match[2].strip() if match[2] else tag
        augmentation = match[3].strip() if match[3] else None
    except Exception:
        print(class_name, image_name, metadata_part)
        raise
    
    # special cases
    if class_name == "Corn_(maize)___Common_rust_":
        # doesn't have UUID, so use the incrementing number
        source_part = source_2
        
    return dict(
            source_1=source_part,
            source_2=source_2,
            tag=tag,
            augmentation=augmentation
        )
            

def list_get(a_list:List, i:int, default:Optional[Any]=None) -> Any:
    """like dict.get but for lists"""
    try:
        return a_list[i]
    except IndexError:
        return default



@dataclass
class Config:
    hdfs_path:str
    output_path:str


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description='Create a manifest table for the unpartitioned dataset.')
    parser.add_argument(
        '--hdfs_path', 
        type=str,
        required=True,
        help='The path to the root of the dataset in HDFS')
    parser.add_argument(
        '--output_path', 
        type=str, 
        required=True,
        help='Output the manifest file to this path')
    
    args = parser.parse_args()
    return Config(
        hdfs_path=args.hdfs_path,
        output_path=args.output_path
    )


if __name__ == "__main__": 
    main()