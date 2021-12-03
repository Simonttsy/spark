package org.apache.spark.ml.fed.model.evaluator

import org.apache.spark.annotation.Since
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.fed.model.ploter.Ploter
import org.apache.spark.ml.functions.checkNonNegativeWeight
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasProbabilityCol, HasWeightCol}
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{Identifiable, MetadataUtils, SchemaUtils}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, Row}

class FedBinaryClassificationEvaluator(override val uid: String) extends Evaluator with HasProbabilityCol with HasLabelCol
  with HasWeightCol {


  def this() = this(Identifiable.randomUID("fedBinaryClassification"))


  override def copy(extra: ParamMap): FedBinaryClassificationEvaluator = defaultCopy(extra)


  def getMetricName: String = $(metricName)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  def setMetricName(value: String): this.type = set(metricName, value)


  /**
    * param for metric name in evaluation (supports `"ksTest"` (default))
    *
    * @group param
    */
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("ksTest"))
    new Param(
      this, "metricName", "metric name in evaluation (ksTest)", allowedParams)
  }

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {

    val metrics = getMetrics(dataset)
    val metric = $(metricName) match {
      case "ksTest" => metrics.kolmogorovSmirnovTest()._2.ks
      case miss => throw new IllegalArgumentException("metric name must in evaluation (ksTest), this is " + miss)
    }

    metric
  }

  def plotKSTestMetrticsResult(dataset: Dataset[_], savePath: String): Unit = {
    val metrics = getMetrics(dataset)

    $(metricName) match {
      case "ksTest" =>
        val kst = metrics.kolmogorovSmirnovTest()
        val ksts = kst._1
        Ploter.KSTest(ksts.map(_.tpr), ksts.map(_.fpr), ksts.map(_.threshold), savePath)
      case miss => throw new IllegalArgumentException("plotKSTestMetrticsResult metric name must in evaluation (ksTest), this is " + miss)
    }
  }

  /**
    * probabilityCol (0.38,0.62)
    *
    * @param dataset
    * @return
    */
  def getMetrics(dataset: Dataset[_]): FedBinaryClassificationMetrics = {

    val schema = dataset.schema

    SchemaUtils.checkColumnTypes(schema, $(probabilityCol), Seq(DoubleType, new VectorUDT))
    //    SchemaUtils.checkColumnType(schema, $(probabilityCol), DoubleType)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    if (isDefined(weightCol)) {
      SchemaUtils.checkNumericType(schema, $(weightCol))
    }


    MetadataUtils.getNumFeatures(schema($(probabilityCol)))
      .foreach(n => require(n == 2, s"probabilityCol vectors must have length=2, but got $n"))

    val scoreAndLabelsWithWeights =
      dataset.select(
        col($(probabilityCol)),
        col($(labelCol)).cast(DoubleType),
        if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0)
        else checkNonNegativeWeight(col($(weightCol)).cast(DoubleType))).rdd.map {
        case Row(probability: Vector, label: Double, weight: Double) =>
          //(probability, label, weight,0.5067392534872546,0.0,1.0)
          //(probability, label, weight,0.4956936642034443,0.0,1.0)
          //          println("probability, label, weight",probability(1), label, weight)
          (probability(1), label, weight)
        case Row(probability: Double, label: Double, weight: Double) =>
          (probability, label, weight)
      }

    new FedBinaryClassificationMetrics(scoreAndLabelsWithWeights)
  }
}
