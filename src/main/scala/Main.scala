import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Spark-EPIC").getOrCreate()
    val sc = spark.sparkContext

    val inputDirectory = "input"
    val inputRDD = sc.textFile(inputDirectory)

    val parsedRDD = parseIntoTuple(inputRDD)

    // For the data processing, we only really care about data from the same company.
    // So we can simplify the problem by considering each company separately.
    val groupedRDD = parsedRDD.groupBy(_._2) // Company column

    // We will also need to process them in order.
    val sortedRDD = groupedRDD.mapValues(_.toList.sortBy(_._1))

    // While grouping by company, we have introduced a key "company", that we don't really need.
    val onlyValuesRDD = sortedRDD.map(_._2) // Second element is the value, so we are effectively dropping the keys.
  }

  private def parseIntoTuple(inputRDD: RDD[String]): RDD[(Int, String, Int)] = {
    inputRDD.map(line => {
      val Array(numberStr, company, valueStr) = line.split(',')
      val number = numberStr.toInt
      val value = valueStr.toInt
      (number, company, value)
    })
  }
}