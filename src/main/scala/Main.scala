import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.commons.io.IOUtils

import java.net.URI


object Main {
  def main(args: Array[String]): Unit = {

    // lettura argomenti
    val bucketPath = args(0)


    val startTime = System.currentTimeMillis()

    val conf = new SparkConf()
      .setAppName("SCP-Project")
    //      .setMaster("local[*]")
    //      .set("spark.driver.memory", "8g")
    //      .set("spark.executor.memory", "8g")
    //      .set("spark.eventLog.enabled", "true")
    //      .set("spark.eventLog.dir", "./logs")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val dataPath = bucketPath + "data/order_products.csv"

    // Lettura del dataset e creazione dell'RDD di Purchase
    val data: RDD[(Int, Int)] = sc.textFile(dataPath)
      .map(line => line.split(","))
      .map(parts => (parts(0).toInt, parts(1).toInt))

    // Creazione di un RDD con orderId -> Set di productId
    val orders: RDD[(Int, Set[Int])] = data
      .combineByKey(
        (prod: Int) => Set(prod), // createCombiner
        (acc: Set[Int], prod: Int) => acc + prod, // mergeValue
        (acc1: Set[Int], acc2: Set[Int]) => acc1 ++ acc2 // mergeCombiners
      )

    // Genera le coppie di prodotti per ogni ordine (senza duplicazioni)
    val pairs: RDD[((Int, Int), Int)] = orders.flatMap { case (_, products) =>
      if (products.size > 1) { // Evita di processare ordini con un solo prodotto
        products.toList.sorted.combinations(2).map {
          case List(p1, p2) => ((p1, p2), 1)
        }
      } else {
        Iterator.empty
      }
    }

    // Somma le occorrenze per ogni coppia di prodotti
    val coPurchaseCounts: RDD[((Int, Int), Int)] = pairs.reduceByKey(_ + _)

    // Trasforma i risultati nel formato CSV "x,y,n"
    val result: RDD[String] = coPurchaseCounts.map { case ((prod1, prod2), count) =>
      s"$prod1,$prod2,$count"
    }

    // Specifica il percorso di output
    val outputPath = bucketPath + "output/co_purchase_analysis"

    result.saveAsTextFile(outputPath)

    // Merge dei file di output in un unico CSV
    mergeOutputFiles(outputPath, args(0) + "output/co_purchase_analysis_single.csv")

    val endTime = System.currentTimeMillis()

    println(s"Time: ${endTime - startTime} ms")
  }

  // Funzione per unire i file di output in un unico CSV
  def mergeOutputFiles(outputDirPath: String, mergedOutputPath: String): Unit = {
    val hadoopConf = new Configuration()
    if (outputDirPath.contains("gs://")){
      hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      hadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    }
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
    } finally {
      outputStream.close()
    }
  }
}
