package org.apache.spark.ml.fed.model.homo

import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{PipelineStage, PredictorParams}
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.fed.constants.Constants
import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.model.base.{FederatedBase, HomoBase}
import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.fed.model.hetero.linr.HeteroLinRModel
import org.apache.spark.ml.fed.model.loss.{DiffFunction, LRLossFunction}
import org.apache.spark.ml.fed.model.optimizer.{Gradient, HomoMiniBatchGradientDescent}
import org.apache.spark.ml.fed.model.param.{FedParams, HasBatchFraction, HasCoefficients, HasRegLambda}
import org.apache.spark.ml.fed.utils.{ParameterTool, Utils}
import org.apache.spark.ml.linalg.{BLAS, DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.VersionUtils.majorMinorVersion

import scala.collection.mutable
import scala.util.Random


trait HomoLRParams extends PredictorParams with FedParams with HasRegParam with HasBatchFraction
  with HasFitIntercept with HasRegLambda with HasStepSize with HasCoefficients with HasMaxIter with HasThreshold {

  setDefault(regParam -> 0.0, batchFraction -> 0.1, threshold -> 0.5)
}

/**
  * Compute hetero-lr loss for:
  *
  *
  * loss = (1/N)*∑(log2 - 1/2*ywx + 1/8*(wx)^2), where y is label, w is model weight and x is features
  *
  * where (wx)^2 = (Wg * Xg + Wh * Xh)^2 = (Wg*Xg)^2 + (Wh*Xh)^2 + 2 * Wg*Xg * Wh*Xh
  *
  * Then loss = log2 - (1/N)*0.5*∑ywx + (1/N)*0.125*[∑(Wg*Xg)^2 + ∑(Wh*Xh)^2 + 2 * ∑(Wg*Xg * Wh*Xh)]
  *
  *
  * 2 guest 不能用PRG抵消，因为另一端guest能反推出对方w,
  * >2 guest 可以用PRG方式，两两配对
  *
  * 做成去中心化的，arbiter随机（默认）起在一台guest上，聚合和均值在普通guest上就行（公钥），arbiter上解密
  */
class HomoLR[V](override val uid: String) extends ProbabilisticClassifier[Vector, HomoLR[V], HomoLRModel]
  with FederatedBase with HomoLRParams with HomoBase with Logging {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("HomoLR"))

  override def copy(extra: ParamMap): HomoLR[V] = defaultCopy(extra)

  var lossAggregator: LRLossFunction = _

  var optimizer: Gradient[V, DiffFunction[V]] = _

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  def setRegParam(value: Double): this.type = set(regParam, value)

  def setRegLambda(value: Double): this.type = set(regLambda, value)

  def setStepSize(value: Double): this.type = set(stepSize, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setBatchFraction(value: Double): this.type = set(batchFraction, value)

  def setFedLabel(value: Boolean): this.type = set(fedLabel, value)

  def setEncryptor(value: Encryptor): this.type = set(encryptor, value)

  def setGuestId(value: String): this.type = set(guestId, value)

  def setThreshold(value: Double): this.type = set(threshold, value)

  def setTrainingSetRatio(value: Double): this.type = set(trainingSetRatio, value)

  /**
    * Train a model using the given dataset and parameters.
    * Developers can implement this instead of `fit()` to avoid dealing with schema validation
    * and copying parameters into the model.
    *
    * @param dataset Training dataset
    * @return Fitted model
    */
  override protected def train(dataset: Dataset[_]): HomoLRModel = {

    null
  }

  def initialize(dataset: Dataset[_]): RDD[FedInstance] = instrumented {
    instr =>
      println("homoLR,trainInternal")

      println("1")
      instr.logPipelineStage(this)
      println("2")

      instr.logDataset(dataset)
      println("3")

      val numFeatures = MetadataUtils.getNumFeatures(dataset, $(featuresCol))
      instr.logNumFeatures(numFeatures)

      println("numFeatures = " + numFeatures)

      println("$(isDefined(coefficients)", isDefined(coefficients))
      val initialCoef = Vectors.dense(if (!isDefined(coefficients)) {
        //Todo 优化，保证均值为0 ，不能正负差异太大
        Array.fill(numFeatures)(Utils.randomGaussian(0.093, 0.095, "0.0000"))
//        Array(0.012,-0.0051,0.033,0.022,-0.016,0.004,-0.009,-0.015,0.008,0.016)
      } else $(coefficients))

      println("initialCoef -> ")
      initialCoef.toArray.foreach(println)

      println("5")
      initializeLossAggregatorAndoptimizer(initialCoef)

      println("start extractFedInstances")
      val df = extractFedInstances(dataset)
      println("end extractFedInstances!!")

      df
  }

  override def initializeLossAggregatorAndoptimizer(initialCoef: Vector): Unit = {
    println("initializeLossAggregatorAndoptimizer for LRLossFunction")

    lossAggregator = new LRLossFunction(getGuestId,
      initialCoef,
      getFitIntercept,
      getRegParam,
      getRegLambda,
      getStepSize,
      getEncryptor)

    println("lossAggregator == null ", lossAggregator == null)
    println("lossAggregator.getClass.getSimpleName", lossAggregator.getClass.getSimpleName)

    println("new MiniBatchGradientDescent")

    optimizer = new HomoMiniBatchGradientDescent[V](getBatchFraction, getMaxIter)
  }


  def setConfFromParamTool(paramTool: ParameterTool): Unit = {

    setFitIntercept(paramTool.get(Constants.ALCHEMY_PIPELINE_INTERCEPT).toBoolean)
    setRegParam(paramTool.get(Constants.ALCHEMY_PIPELINE_REGULARIZATION).toDouble)
    setRegLambda(paramTool.get(Constants.ALCHEMY_PIPELINE_SCALE_REGULARIZATION).toDouble)
    setStepSize(paramTool.get(Constants.ALCHEMY_PIPELINE_STEP_SIZE).toDouble)
    setMaxIter(paramTool.get(Constants.ALCHEMY_PIPELINE_MAX_ITER).toInt)
    setBatchFraction(paramTool.get(Constants.ALCHEMY_PIPELINE_BATCH_FRACTION).toDouble)
    setGuestId(paramTool.get(Constants.GUEST_ID))
    setTrainingSetRatio(paramTool.get(Constants.ALCHEMY_PIPELINE_TRAINING_SET_RATIO).toDouble)
    //depercted
    setFedLabel(true)

  }

  override def fedFit(): Unit = {

  }

  /**
    * Random.nextInt(1) -> 0
    * Random.nextInt(2) -> 1/2
    * ....
    *
    * @param guestIdArray
    * @return
    */
  def selectAggrSideRandom(guestIdArray: Array[String]): String = {

    guestIdArray(Random.nextInt(guestIdArray.length))
  }

}

class HomoLRModel(override val uid: String,
                  override val numClasses: Int = 2,
                  val coefficients: Vector,
                  val intercept: Double,
                  val threshold: Double = 0.5) extends ProbabilisticClassificationModel[Vector, HomoLRModel]
  with GeneralMLWritable {


  /**
    * Estimate the probability of each class given the raw prediction,
    * doing the computation in-place.
    * These predictions are also called class conditional probabilities.
    *
    * This internal method is used to implement `transform()` and output [[probabilityCol]].
    *
    * @return Estimated class conditional probabilities (modified input vector)
    */
  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        val values = dv.values
        values(0) = 1.0 / (1.0 + math.exp(-values(0)))
        values(1) = 1.0 - values(0)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }

  /**
    * Raw prediction for each possible label.
    * The meaning of a "raw" prediction may vary between algorithms, but it intuitively gives
    * a measure of confidence in each possible label (where larger = more confident).
    * This internal method is used to implement `transform()` and output [[rawPredictionCol]].
    *
    * @return vector where element i is the raw prediction for label i.
    *         This raw prediction may be any real number, where a larger value indicates greater
    *         confidence for that label.
    */
  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  override def copy(extra: ParamMap): HomoLRModel = ???


  /** Margin (rawPrediction) for class label 1.  For binary classification only. */
  private val margin: Vector => Double = features => {
    BLAS.dot(features, coefficients) + intercept
  }


  /** Score (probability) for class label 1.  For binary classification only. */
  private val score: Vector => Double = features => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }

  override def predict(features: Vector): Double = {

    //    1.0 / (1.0 + math.exp(-(features.dot(coefficients) + intercept)))
    if (score(features) > threshold) 1 else 0
  }

  /**
    * Returns an `MLWriter` instance for this ML instance.
    */
  override def write: GeneralMLWriter = new GeneralMLWriter(this)
}


/** A writer for HomoLRModel that handles the "internal" (or default) format
  *
  * put path -> E:....\spark-master\mllib\src\main\resources\META-INF\services\org.apache.spark.ml.util.MLFormatRegister
  * */
private class InternalHomoLRModelWriter
  extends MLWriterFormat with MLFormatRegister {

  override def format(): String = "internal"

  override def stageName(): String = "org.apache.spark.ml.fed.model.homo.HomoLRModel"

  private case class Data(intercept: Double, coefficients: Vector)

  override def write(path: String, sparkSession: SparkSession,
                     optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    val instance = stage.asInstanceOf[HomoLRModel]
    val sc = sparkSession.sparkContext
    // Save metadata and Params
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    // Save model data: intercept, coefficients, scale
    val data = Data(instance.intercept, instance.coefficients)
    val dataPath = new Path(path, "data").toString
    sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
  }
}

object HomoLRModel extends MLReadable[HomoLRModel] {

  val numClasses: Int = 2

  def createModel(uid: String, coefficients: Vector, intercept: Double): HomoLRModel = {
    new HomoLRModel(uid, numClasses, coefficients, intercept)
  }

  /**
    * Returns an `MLReader` instance for this class.
    */
  override def read: MLReader[HomoLRModel] = new HomoLRModelReader

  @Since("1.6.0")
  override def load(path: String): HomoLRModel = super.load(path)

  private class HomoLRModelReader extends MLReader[HomoLRModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[HeteroLinRModel].getName

    override def load(path: String): HomoLRModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val (majorVersion, minorVersion) = majorMinorVersion(metadata.sparkVersion)
      val model = if (majorVersion < 2 || (majorVersion == 2 && minorVersion <= 2)) {
        // Spark 2.2 and before
        val Row(intercept: Double, coefficients: Vector) =
          MLUtils.convertVectorColumnsToML(data, "coefficients")
            .select("intercept", "coefficients")
            .head()
        new HomoLRModel(metadata.uid, numClasses, coefficients, intercept)
      } else {
        // Spark 2.3 and later
        val Row(intercept: Double, coefficients: Vector) =
          data.select("intercept", "coefficients").head()
        new HomoLRModel(metadata.uid, numClasses, coefficients, intercept)
      }

      metadata.getAndSetParams(model)
      model
    }
  }

}