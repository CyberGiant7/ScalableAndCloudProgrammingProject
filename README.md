# Scalable and Cloud Programming Project

This project demonstrates how to use Google Cloud Dataproc and Spark to perform a co-purchase analysis on a dataset of order products.

## Prerequisites

Before you begin, ensure you have the following:

- Google Cloud SDK installed
- An active Google Cloud account
- SBT (Scala Build Tool) installed

## Setup

1. **Create a Dataproc Cluster**

   Replace `your-cluster-name` and `your-region` with your desired cluster name and region.

   ```sh
   gcloud dataproc clusters create your-cluster-name --region=your-region --num-workers=2 --master-boot-disk-size=240 --worker-boot-disk-size=240
   ```

2. **Create a Google Cloud Storage Bucket**

   Replace `your-bucket-name` with your desired bucket name.

   ```sh
   gcloud storage buckets create gs://your-bucket-name
   ```

3. **Package the Project with SBT**

   Navigate to the project directory and run:

   ```sh
   sbt package
   ```

4. **Submit the Spark Job**

   Replace `your-cluster-name`, `your-region`, and `your-bucket-name` with your cluster name, region, and bucket name respectively.

   ```sh
   gcloud dataproc jobs submit spark --cluster=your-cluster-name --region=your-region --jar=gs://your-bucket-name/scalableproject_2.12-0.1.jar -- gs://your-bucket-name/
   ```

5. **Submit the Spark Job with Custom Properties**

   Replace `your-cluster-name`, `your-region`, and `your-bucket-name` with your cluster name, region, and bucket name respectively.

   ```sh
   gcloud dataproc jobs submit spark --cluster=your-cluster-name --region=your-region --properties=spark.executor.instances=1,spark.executor.cores=4 --jar=gs://your-bucket-name/scalableproject_2.12-0.1.jar -- gs://your-bucket-name/
   ```

## Project Structure

- `src/main/scala/Main.scala`: The main Spark application code.
- `data/order_products.csv`: The dataset used for the co-purchase analysis.

## Execution

The Spark job performs the following steps:

1. Reads the dataset from the specified Google Cloud Storage bucket.
2. Processes the data to generate co-purchase pairs.
3. Saves the results back to the Google Cloud Storage bucket.

## Metrics

The job also collects and saves execution metrics, including the number of executors, cores per executor, executor memory, and execution time.

## License

This project is licensed under the MIT License.
