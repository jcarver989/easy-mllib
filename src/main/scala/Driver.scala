import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.DataFrame
import MLPipeline.{ Categorical, ClassificationLabel, Numerical, WordBag }

object Driver extends App with LocalContext {
  import sql.implicits._

  val pipeline = new MLPipeline(
    // we try to predict this
    "Category" -> ClassificationLabel,

    // using these
    "Account" -> Categorical,
    "Description" -> WordBag,
    "Amount" -> Numerical)

  val model = pipeline.train(
    loadData("src/main/resources/transactions.csv"),
    new RandomForestClassifier() // can easily swap this out with say new LogisticRegression()
    )

  // Simple case class we can transform to a DataFrame for tests
  case class TestExample(Account: String, Description: String, Amount: Double, Category: String = "Deposits")
  val singleExample = sc.parallelize(Seq(
    TestExample(
      Account = "Credit Card",
      Description = "Amazon.com",
      Amount = -41.33))).toDF

  // "transform" makes predictions by appending columns to the original Dataframe
  val results = model.transform(singleExample)

  // e.x. PredictionLabel 
  results.select($"PredictionLabel").collect().foreach { println } // Should get 'General Merchandise'

  private def loadData(path: String): DataFrame = {
    // Expected format: csv file with this header line:
    // "Date","Account","Description","Category","Tags","Amount"
    sql
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      // drop columns we don't care about
      .drop($"Date")
      .drop($"Tags")
  }
}