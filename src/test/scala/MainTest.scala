import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.flatspec._
import org.scalatest.BeforeAndAfterAll
import Main.solveForRDD
import org.apache.spark.sql.SparkSession

class MainTest extends AnyFlatSpec with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Spark-Tests").master("local[*]").getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null)
      spark.stop()
  }

  "If given empty RDD" should "output should be empty" in {
    val emptyRDD = spark.sparkContext.emptyRDD[String]
    val outputRDD = solveForRDD(emptyRDD)
    assert(outputRDD.isEmpty())
  }
}
