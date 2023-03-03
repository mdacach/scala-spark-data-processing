import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Spark-EPIC").getOrCreate()
    val sc = spark.sparkContext
  }
}