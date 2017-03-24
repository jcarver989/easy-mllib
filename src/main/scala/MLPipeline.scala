import org.apache.spark.ml.{ Pipeline, PipelineModel, PipelineStage, Predictor }
import org.apache.spark.ml.feature.{ HashingTF, IDF, IndexToString, StringIndexer, StringIndexerModel, Tokenizer, VectorAssembler }
import org.apache.spark.sql.DataFrame
import MLPipeline._

object MLPipeline {
  // Types of feature columns
  sealed trait FeatureType
  case object WordBag extends FeatureType
  case object Categorical extends FeatureType
  case object Numerical extends FeatureType

  // Types of target lables
  sealed trait LabelType
  case object RegressionLabel extends LabelType
  case object ClassificationLabel extends LabelType
}

/**
 * Wraps SparkML to provide an easier to use API to train models with Dataframes.
 *
 *  You just create a new MLPipeline and pass the column names with their corresponding types, then call train(...).
 *  See Driver.scala for an example
 */
class MLPipeline(target: (String, LabelType), rawFeatureColumns: (String, FeatureType)*) {
  def train[T <: Predictor[_, T, _]](df: DataFrame, predictor: T): PipelineModel = {
    // If our target label is a category, transform it to an index
    val (targetName, targetColumnType) = target
    val targetIndexer = targetColumnType match {
      case ClassificationLabel => Some(indexCategorical(targetName).fit(df))
      case _                   => None
    }

    // If our target label is a category, add a column to transform the index back to the original column
    val predictionTransformer = targetIndexer.map { model => predictionLabel(model) }

    // Transform each non numerical raw feature into a vector representation
    val nameWithTransformer = rawFeatureColumns.map {
      case (name, WordBag)     => s"${name}TFIDF" -> Some(tfIDF(name))
      case (name, Categorical) => s"${name}Index" -> Some(Vector(indexCategorical(name)))
      case (name, Numerical)   => name -> None
    }

    val featureNames = nameWithTransformer.map { _._1 }
    val featureTransformers = nameWithTransformer.flatMap { _._2 }.flatten

    predictor
      .setFeaturesCol("features")
      .setLabelCol(predictionTransformer.map { t => s"${targetName}Index" }.getOrElse(targetName))

    val stages = featureTransformers ++
      Vector(assembleFeatures(featureNames: _*)) ++
      targetIndexer.toVector ++
      Vector(predictor) ++
      predictionTransformer.toVector

    // train the model
    new Pipeline().setStages(stages.toArray).fit(df)
  }

  private def tfIDF(inputCol: String): Seq[PipelineStage] = {
    val tokenizer = new Tokenizer()
      .setInputCol(inputCol)
      .setOutputCol(s"${inputCol}Tokens")

    val hashingTF = new HashingTF()
      .setInputCol(s"${inputCol}Tokens")
      .setOutputCol(s"${inputCol}TF").setNumFeatures(20)

    val idf = new IDF()
      .setInputCol(s"${inputCol}TF")
      .setOutputCol(s"${inputCol}TFIDF")

    Vector(tokenizer, hashingTF, idf)
  }

  private def indexCategorical(inputCol: String): StringIndexer = {
    new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(s"${inputCol}Index")
  }

  private def predictionLabel(indexer: StringIndexerModel): IndexToString = {
    new IndexToString()
      .setInputCol(s"prediction")
      .setOutputCol(s"PredictionLabel")
      .setLabels(indexer.labels)
  }

  private def assembleFeatures(inputCols: String*): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(Array(inputCols: _*))
      .setOutputCol("features")
  }
}