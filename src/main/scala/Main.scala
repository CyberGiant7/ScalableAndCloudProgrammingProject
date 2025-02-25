import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


object Main {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SCP-Project")
      .setMaster("local[*]")
    //      .set("spark.driver.extraJavaOptions", "--add-opens java.base/sun.nio.ch=ALL-UNNAMED")
    //      .set("spark.executor.extraJavaOptions", "--add-opens java.base/sun.nio.ch=ALL-UNNAMED")

    //    println("Configurazione Spark:")
    //    println(conf.getAll.mkString("\n"))

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val dataPath = "./data/order_products.csv"

    //    case class Purchase(orderId: Int, productId: Int)
    //
    //    val data: RDD[Purchase] = sc.textFile(dataPath)
    //      .map((line: String) => line.split(","))
    //      .map((line: Array[String]) => Purchase(line(0).toInt, line(1).toInt))
    //
    //    data.foreach(println)
  }
}
