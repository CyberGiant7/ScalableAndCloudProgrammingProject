import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.commons.io.IOUtils

import java.net.URI

object Main {
  def main(args: Array[String]): Unit = {
    val bucketPath = args(0)

    val startTime = System.currentTimeMillis()

    val conf = new SparkConf()
      .setAppName("SCP-Project")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    // Get cluster information: number of executors, cores per executor, and memory per executor
    val numExecutors = sc.getConf.get("spark.executor.instances", "unknown")
    val numCores = sc.getConf.get("spark.executor.cores", "unknown")
    val executorMemory = sc.getConf.get("spark.executor.memory", "unknown")
    val partitionNumber = (numExecutors.toInt * numCores.toInt * 2).toInt

    println(s"💻 Cluster info: Executors: $numExecutors | Cores per Executor: $numCores | Executor Memory: $executorMemory")

    val dataPath = bucketPath + "data/order_products.csv"

    // Read the dataset and create an RDD of (orderId, productId) pairs
    val data: RDD[(Int, Int)] = sc.textFile(dataPath, partitionNumber)
      .map(line => line.split(","))
      .map(parts => (parts(0).toInt, parts(1).toInt))

    println(s"📊 Number of partitions in the dataset: ${data.getNumPartitions}")

    // Create an RDD with orderId -> Set of productIds
    val orders: RDD[(Int, Set[Int])] = data.
      combineByKey(
        (prod: Int) => Set(prod), // createCombiner
        (acc: Set[Int], prod: Int) => acc + prod, // mergeValue
        (acc1: Set[Int], acc2: Set[Int]) => acc1 ++ acc2, // mergeCombiners
        partitionNumber
      )

    // Generate product pairs for each order (avoid duplicates)
    val pairs: RDD[((Int, Int), Int)] = orders.flatMap { case (_, products) =>
      if (products.size > 1) {
        products.toList.sorted.combinations(2).map {
          case List(p1, p2) => ((p1, p2), 1)
        }
      } else {
        Iterator.empty
      }
    }

    println(s"📊 Number of partitions after mapping: ${pairs.getNumPartitions}")

    // Sum occurrences for each product pair
    val coPurchaseCounts: RDD[((Int, Int), Int)] = pairs.reduceByKey(_ + _, partitionNumber)

    // Convert results into CSV format "x,y,n"
    val result: RDD[String] = coPurchaseCounts.map { case ((prod1, prod2), count) =>
      s"$prod1,$prod2,$count"
    }

    // Define the output path
    val outputPath = bucketPath + "output/co_purchase_analysis"
    result.saveAsTextFile(outputPath)

    val hadoopConf = new Configuration()
    hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    // Merge all output part files into a single CSV file
    val mergedOutputPath = bucketPath + "output/co_purchase_analysis_single.csv"
    mergeOutputFiles(hadoopConf, outputPath, mergedOutputPath)

    val endTime = System.currentTimeMillis()
    val executionTime = endTime - startTime
    println(s"⏱ Execution Time: $executionTime ms")

    // Save execution metrics to a file on GCS
    val metricsPath = bucketPath + "output/metrics.txt"
    saveMetricsToGCS(hadoopConf, metricsPath, numExecutors, numCores, executorMemory, executionTime, partitionNumber)
  }

  // Function to merge multiple output part files into a single CSV file
  private def mergeOutputFiles(hadoopConf: Configuration, outputDirPath: String, mergedOutputPath: String): Unit = {
    val fs = FileSystem.get(new URI(mergedOutputPath), hadoopConf)
    val outputDir = new Path(outputDirPath)
    val mergedOutput = new Path(mergedOutputPath)

    val outputStream: FSDataOutputStream = fs.create(mergedOutput)
    try {
      val partFiles = fs.listStatus(outputDir)
        .map(_.getPath)
        .filter(_.getName.startsWith("part-"))
        .sortBy(_.getName)

      partFiles.foreach { path =>
        val inputStream = fs.open(path)
        try {
          IOUtils.copy(inputStream, outputStream)
        } finally {
          inputStream.close()
        }
      }
      println(s"✅ Merged output saved to: $mergedOutputPath")
    } finally {
      outputStream.close()
    }
  }

  // Function to save execution metrics to a file on GCS
  private def saveMetricsToGCS(hadoopConf: Configuration, outputPath: String, executors: String, cores: String, memory: String, execTime: Long, partitionNumber: Int): Unit = {
    val fs = FileSystem.get(new URI(outputPath), hadoopConf)
    val output = new Path(outputPath)
    val outputStream: FSDataOutputStream = fs.create(output)

    try {
      val content =
        s"""Spark Job Metrics:
           |Executors: $executors
           |Cores per Executor: $cores
           |Partitions: $partitionNumber
           |Executor Memory: $memory
           |Execution Time (ms): $execTime
           |""".stripMargin

      outputStream.writeBytes(content)
      println(s"✅ Metrics saved to $outputPath")
    } finally {
      outputStream.close()
    }
  }
}
