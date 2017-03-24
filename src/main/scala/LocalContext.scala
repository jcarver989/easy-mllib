import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

trait LocalContext {
  val conf = new SparkConf()
  val sc = new SparkContext("local[4]", "Driver", conf)
  val sql = new SQLContext(sc)
}
