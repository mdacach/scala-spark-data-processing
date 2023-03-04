import Main.solveForRDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec._

class MainTest extends AnyFlatSpec with BeforeAndAfterAll {
  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    sc = SparkSession.builder().appName("Spark-Tests").master("local[*]").getOrCreate().sparkContext
  }

  override def afterAll(): Unit = {
    if (sc != null)
      sc.stop()
  }

  "If given empty RDD" should "output should be empty" in {
    val emptyRDD = sc.emptyRDD[(Int, String, Int)]
    val outputRDD = solveForRDD(emptyRDD)
    assert(outputRDD.isEmpty())
  }

  "If each company only has one entry" should "all answers are 0" in {
    val data = Array(
      (1, "APPLE", 2000),
      (2, "GOOGLE", 10),
      (3, "MICROSOFT", 5000),
    )
    val rdd = sc.parallelize(data)

    // TODO: it's better if we test only the answer portion of the output, instead of the whole entries.
    val outputRDD = solveForRDD(rdd)
    val outputData = outputRDD.collect()
    val correct = Array(
      (1, "APPLE", 2000, 0, 0),
      (2, "GOOGLE", 10, 0, 0),
      (3, "MICROSOFT", 5000, 0, 0),
    )
    assert(outputData sameElements correct)
  }

  "If no values are bigger than 1000" should "all answers are 0" in {
    val data = Array(
      (1, "APPLE", 1000),
      (2, "GOOGLE", 10),
      (3, "MICROSOFT", 500),
      (4, "GOOGLE", 100),
      (5, "MICROSOFT", 25),
      (6, "MICROSOFT", 850),
      (7, "DELTIX", 1000),
    )
    val rdd = sc.parallelize(data)

    // TODO: it's better if we test only the answer portion of the output, instead of the whole entries.
    val outputRDD = solveForRDD(rdd)
    val outputData = outputRDD.collect()
    val correct = Array(
      (1, "APPLE", 1000, 0, 0),
      (2, "GOOGLE", 10, 0, 0),
      (3, "MICROSOFT", 500, 0, 0),
      (4, "GOOGLE", 100, 0, 0),
      (5, "MICROSOFT", 25, 0, 0),
      (6, "MICROSOFT", 850, 0, 0),
      (7, "DELTIX", 1000, 0, 0),
    )
    assert(outputData sameElements correct)
  }

  "If all values are bigger than 1000" should "only the first answer for each company is 0" in {
    val data = Array(
      (1, "APPLE", 2000),
      (2, "GOOGLE", 2000),
      (3, "MICROSOFT", 2000),
      (4, "GOOGLE", 2000),
      (5, "MICROSOFT", 2000),
      (6, "DELTIX", 2000),
      (7, "APPLE", 2000),
    )
    val rdd = sc.parallelize(data)

    // TODO: it's better if we test only the answer portion of the output, instead of the whole entries.
    val outputRDD = solveForRDD(rdd)
    val outputData = outputRDD.collect()
    val correct = Array(
      (1, "APPLE", 2000, 0, 0),
      (2, "GOOGLE", 2000, 0, 0),
      (3, "MICROSOFT", 2000, 0, 0),
      (4, "GOOGLE", 2000, 2, 2000),
      (5, "MICROSOFT", 2000, 3, 2000),
      (6, "DELTIX", 2000, 0, 0),
      (7, "APPLE", 2000, 1, 2000),
    )
    assert(outputData sameElements correct)
  }

  "If some values are bigger than 1000" should "answers correct indicate the previous valid entry" in {
    val data = Array(
      (1, "APPLE", 2000),
      (2, "GOOGLE", 10),
      (3, "MICROSOFT", 5000),
      (4, "APPLE", 100),
      (5, "GOOGLE", 2000),
      (6, "MICROSOFT", 3000),
      (7, "GOOGLE", 100),
      (8, "GOOGLE", 200),
    )
    val rdd = sc.parallelize(data)

    // TODO: it's better if we test only the answer portion of the output, instead of the whole entries.
    val outputRDD = solveForRDD(rdd)
    val outputData = outputRDD.collect()
    val correct = Array(
      (1, "APPLE", 2000, 0, 0),
      (2, "GOOGLE", 10, 0, 0),
      (3, "MICROSOFT", 5000, 0, 0),
      (4, "APPLE", 100, 1, 2000),
      (5, "GOOGLE", 2000, 0, 0),
      (6, "MICROSOFT", 3000, 3, 5000),
      (7, "GOOGLE", 100, 5, 2000),
      (8, "GOOGLE", 200, 5, 2000),
    )
    assert(outputData sameElements correct)
  }
}
