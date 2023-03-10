import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Spark-EPIC").getOrCreate()
    solve(spark, "input", "output")
  }

  def solve(sparkSession: SparkSession, inputDirectory: String, outputDirectory: String): Unit = {
    val sc = sparkSession.sparkContext

    // Read is an IO function, let's have it at this layer and keep "solveForRDD" pure.
    val inputRDD = sc.textFile(inputDirectory)
    // Parsing ended up more convenient to have here too.
    val parsedRDD = parseIntoTuple(inputRDD)
    val outputRDD = solveForRDD(parsedRDD)

    // Save to CSV files.
    val asDataFrame = sparkSession.createDataFrame(outputRDD)
    asDataFrame.write.format("csv").save(outputDirectory)
    // The challenge asked for a particular naming convention for output files:
    // output-1.csv, output-2.csv, and so on...
    // but it does not seem straightforward to do that with Spark
    // Possible Solutions:
    // 1. Rename all output files in place.
    //    (But maybe Spark uses the naming conventions for something and we should not change it?)
    // 2. Copy all the output files and rename the copies.
    //    (Bad because we would be using 2x the amount of storage to save the files, which can be unfeasible)
  }

  // Pure function, easier to test and reason about.
  def solveForRDD(inputRDD: RDD[InitialDataTuple]): RDD[ProcessedDataTuple] = {
    // For the data processing, we only really care about data from the same company.
    // So we can simplify the problem by considering each company separately.
    val groupedRDD = inputRDD.groupBy(_._2) // Company column

    // We will also need to process them in order.
    val sortedRDD = groupedRDD.mapValues(_.toList.sortBy(_._1))

    // While grouping by company, we have introduced a key "company", that we don't really need.
    val onlyValuesRDD = sortedRDD.map(_._2) // Second element is the value, so we are effectively dropping the keys.

    val neighborsRDD = processNeighbors(onlyValuesRDD)

    val propagatedRDD = propagateAnswers(neighborsRDD)
    // Now we have solved the problem for each company separately.

    val outputRDD = propagatedRDD.flatMap(x => x).sortBy(_._1)
    outputRDD
  }

  type InitialDataTuple = (Int, String, Int)

  private def parseIntoTuple(inputRDD: RDD[String]): RDD[InitialDataTuple] = {
    inputRDD.map(line => {
      val Array(numberStr, company, valueStr) = line.split(',')
      val number = numberStr.toInt
      val value = valueStr.toInt
      (number, company, value)
    })
  }

  type ProcessedDataTuple = (Int, String, Int, Int, Int)

  // If the current entry is valid (has value > 1000, in this instance)
  // it will *always* be the answer for the next entry.
  // Consider
  // (1, Microsoft, 1500)
  // for entry 2, the answer will be (_, _, _, 1, 1500)
  // so we can already gather these "local" answers.
  private def processNeighbors(inputRDD: RDD[List[InitialDataTuple]]): RDD[List[ProcessedDataTuple]] = {
    inputRDD.map(company => {
      val (firstID, firstName, firstValue) = company.head
      val firstEntry = (firstID, firstName, firstValue, 0, 0)

      // In the case there is only one element, the answer is just the first entry.
      if (company.size == 1) {
        List(firstEntry)
      } else {
        // Note that because our sliding window yields the "second" entry,
        // the first entry will be skipped, and should be restored later.
        val secondEntryOnwards = company.sliding(2).map {
          case Seq((previousID, previousCompany, previousValue), (currentID, currentCompany, currentValue)) =>
            // As we have grouped by company, the previousCompany here should always match currentCompany
            assert(previousCompany == currentCompany, "Neighbors should have the same company name.")

            // We can easily switch this requirement in the future.
            // To make it more generic, we could receive a "validatorFunction" by argument and use it
            // here.
            val isValid = previousValue > 1000
            if (isValid)
            {
              (currentID, currentCompany, currentValue, previousID, previousValue) // it's the answer for `current`
            } else {
              (currentID, currentCompany, currentValue, 0, 0) // we still do not know the answer for this entry
            }
        }
        (Iterator(firstEntry) ++ secondEntryOnwards).toList
      }
    })
  }

  // In general, the answer for entry `i` will be either:
  // 1. Entry `i-1`, if it is valid (value > 1000)
  // 2. The answer for entry `i-1`, that we have already calculated.
  // By processing the tuples in order, we can propagate the answers throughout the whole RDD.
  private def propagateAnswers(inputRDD: RDD[List[ProcessedDataTuple]]): RDD[List[ProcessedDataTuple]] = {
    inputRDD.map(company => {
      // In the case there is only one element, the answer is just the first entry.
      if (company.size == 1) {
        List(company.head)
      } else { // As above, the first entry will be skipped and should be restored later.
        val secondEntryOnwards = company.sliding(2).map {
          case Seq((previousID, previousCompany, previousValue, previousAnswerID, previousAnswerValue), (currentID, currentCompany, currentValue, currentAnswerID, currentAnswerValue)) =>
            // As we have grouped by company, the previousCompany here should always match currentCompany
            assert(previousCompany == currentCompany, "Neighbors should have the same company name.")

            val doNotHaveAnswerForCurrent = currentAnswerID == 0
            if (doNotHaveAnswerForCurrent) {
              // It's always OK to populate it with the previous answers.
              // 1. If previous answer does not exist, it is 0 and we do not change anything.
              // 2. If previous answer exists, we correctly propagate it to the current entry.
              (currentID, currentCompany, currentValue, previousAnswerID, previousAnswerValue) // it's the answer for `current`
            } else {
              // This answer is already OK. Do not change it.
              (currentID, currentCompany, currentValue, currentAnswerID, currentAnswerValue)
            }
        }
        (Iterator(company.head) ++ secondEntryOnwards).toList
      }
    })
  }
}