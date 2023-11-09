from tensorflow.keras.applications import VGG16
from tensorflow.keras.layers import Dense, Flatten
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark import SparkContext
import matplotlib.pyplot as plt
import matplotlib
import os
import numpy as np

def test_program():
    # Check if your spark is setup correctly 
    # spark = SparkSession.builder.appName("PySparkTest").getOrCreate()

    try:
        data = [("Alice", 28), ("Bob", 22), ("Charlie", 32)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, columns)
        df.show()
        # spark.stop()
    except Exception as e:
        print("An error occurred: ", e)

def create_pre_trained_cnn():
    # load pre-trained model
    IMAGE_SIZE = (256,256,3) #TODO: WHAT IS THE SIZE OF OUR IMAGES?
    base_model = VGG16(weights='imagenet', include_top=False, input_shape=IMAGE_SIZE) 

    NUM_CLASSES = 38
    x = Flatten()(base_model.output)
    x = Dense(256, activation='relu')(x)
    output = Dense(NUM_CLASSES, activation='softmax')(x)

    # create model
    model = Model(inputs=base_model.input, outputs=output)
    model.compile(optimizer="adam", loss="categorical_crossentropy", metrics=["accuracy"])

    return model

def load_data():
    hdfs_path = "hdfs:///CS435_Plant_Data/"
    # /CS435_Plant_Data/

    # Since data is split into subdirectories specifying classes we will recursively search subdirectories
    train_data = spark.read.format("image").option("recursiveFileLookup", "true").load(hdfs_path + "train/")
    train_data = train_data.withColumn("label", regexp_replace(input_file_name(), r"/[^/]+$", ""))

    test_data = spark.read.format("image").load(hdfs_path + "test/")

    validation_data = spark.read.format("image").option("recursiveFileLookup", "true").load(hdfs_path + "valid/")
    validation_data = validation_data.withColumn("label", regexp_replace(input_file_name(), r"/[^/]+$", ""))
    
    return train_data, test_data, validation_data

def plot_label_distribution(df):
    # Group the DataFrame by the "label" column and count the occurrences of each class
    # Create a new column "clean_label" by removing the prefix
    df = df.withColumn("clean_label", regexp_replace("label", r'^hdfs://[^/]+', ""))
    df = df.withColumn("clean_label", regexp_replace("clean_label", r"/CS435_Plant_Data/train/[a-zA-Z]+/", ""))
    df = df.withColumn("clean_label", regexp_replace("clean_label", r"_+", " "))

    # Get the unique clean labels
    unique_clean_labels = [row.clean_label for row in df.select("clean_label").distinct().collect()]

    # Count the number of labels for each unique "plant"
    plant_counts = {}
    plant_health_counts = {}

    for label in unique_clean_labels:
        plant = label.split()[0]  # Extract the first word (plant name)
        if plant in plant_counts:
            plant_counts[plant] += 1
        else:
            plant_counts[plant] = 1

        # Check if the label contains "healthy" to determine health status
        if "healthy" in label:
            if plant in plant_health_counts:
                plant_health_counts[plant]["healthy"] += 1
            else:
                plant_health_counts[plant] = {"healthy": 1, "diseased": 0}
        else:
            if plant in plant_health_counts:
                plant_health_counts[plant]["diseased"] += 1
            else:
                plant_health_counts[plant] = {"healthy": 0, "diseased": 1}

    # Plot the count of healthy and diseased labels for each plant
    plant_names_health = list(plant_health_counts.keys())
    plant_healthy_counts = [health_counts["healthy"] for health_counts in plant_health_counts.values()]
    plant_diseased_counts = [health_counts["diseased"] for health_counts in plant_health_counts.values()]

    plt.figure(figsize=(12, 6))
    plt.bar(plant_names_health, plant_healthy_counts, label="Healthy")
    plt.bar(plant_names_health, plant_diseased_counts, bottom=plant_healthy_counts, label="Diseased")
    plt.xlabel("Plant")
    plt.ylabel("Label Count")
    plt.title("Healthy and Diseased Label Count for Each Plant")
    plt.xticks(rotation=45, ha="right")
    plt.legend()

    plt.subplots_adjust(hspace=1)
    
    if os.path.exists('Individual_Healthy_Diseased_Class_Distribution.png'):
        os.remove('Individual_Healthy_Diseased_Class_Distribution.png')

    plt.savefig('Individual_Healthy_Diseased_Class_Distribution.png')

    plt.figure(figsize=(12,6))
    total_healthy_count = sum(plant_healthy_counts)
    total_diseased_count = sum(plant_diseased_counts)
    plt.bar(["Healthy", "Diseased"], [total_healthy_count, total_diseased_count])
    plt.xlabel("Overall Health Status")
    plt.ylabel("Label Count")
    plt.title("Overall Healthy vs. Diseased Label Count")

    if os.path.exists('Aggregate_Healthy_Diseased_Class_Distribution.png'):
        os.remove('Aggregate_Healthy_Diseased_Class_Distribution.png')

    plt.savefig('Aggregate_Healthy_Diseased_Class_Distribution.png')

def train_model(model, train, num_epochs=10, batch_size=32):
    ## train the model
    history = ""
    return model, history

if __name__ == "__main__": 
    # RUNNING VIA SPARK: $SPARK_HOME/bin/spark-submit PD_Classifier.py
    # initialize a Spark session
    spark = SparkSession.builder.appName("CNN Training").getOrCreate()

    # test_program()

    pretrained_cnn = create_pre_trained_cnn()

    train, test, validation = load_data()
    train.show()

    plot_label_distribution(train)

    trained_cnn, history = train_model(pretrained_cnn, train, num_epochs=1, batch_size=32)

    spark.stop()