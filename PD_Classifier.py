from tensorflow.keras.applications import VGG16
from tensorflow.keras.layers import Dense, Flatten
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, when, sum as spark_sum, input_file_name, col
from pyspark import SparkContext
import matplotlib.pyplot as plt
import matplotlib
import os
import numpy as np

def load_data():
    hdfs_path = "hdfs:///cs435/CS435_Plant_Data/"

    # Since data is split into subdirectories specifying classes we will recursively search subdirectories
    train_data = spark.read.format("image").option("recursiveFileLookup", "true").load(hdfs_path + "train/")
    train_data = train_data.withColumn("label", regexp_replace(input_file_name(), r"/[^/]+$", ""))

    test_data = spark.read.format("image").load(hdfs_path + "test/")

    validation_data = spark.read.format("image").option("recursiveFileLookup", "true").load(hdfs_path + "valid/")
    validation_data = validation_data.withColumn("label", regexp_replace(input_file_name(), r"/[^/]+$", ""))
    
    return train_data, test_data, validation_data

def plot_label_distribution(df):
    label_counts = df.groupBy('label').count().orderBy('count', ascending=False).collect()
    labels = [row['label'] for row in label_counts]
    counts = [row['count'] for row in label_counts]

    labels = [label.split('/')[-1] for label in labels]
    plt.figure(figsize=(10, 6))
    plt.bar(labels, counts, color='skyblue')
    plt.xlabel('Classes')
    plt.ylabel('Number of Images')
    plt.title('Distribution of Images per Class')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('new_class_distributions.png', bbox_inches='tight')

def plot_healthy_vs_diseased_distribution(df):
    # Get the unique clean labels
    unique_clean_labels = [row.label for row in df.select("label").distinct().collect()]

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
    

if __name__ == "__main__": 
    # RUNNING VIA SPARK: $SPARK_HOME/bin/spark-submit PD_Classifier.py
    # initialize a Spark session
    spark = SparkSession.builder.appName("CNN Training").getOrCreate()

    train, test, validation = load_data()
    train.show()

    plot_label_distribution(train)

    # plot_healthy_vs_diseased_distribution(train)

    spark.stop()
    
