package org.apache.spark.ml.fed.model.hetero.linr

import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.fed.constants.Constants
import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.model.base.{FederatedBase, HeteroBase}
import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.fed.model.loss.{DiffFunction, FedDiffLossAggregator, FedLinRDiffLossAggregator}
import org.apache.spark.ml.fed.model.optimizer.{Gradient, HeteroMiniBatchGradientDescent}
import org.apache.spark.ml.fed.model.param._
import org.apache.spark.ml.fed.utils.{ParameterTool, Utils}
import org.apache.spark.ml.linalg.BLAS.dot
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.regression.{RegressionModel, Regressor}
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.ml.util._
import org.apache.spark.ml.{PipelineStage, PredictorParams}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.VersionUtils.majorMinorVersion

import scala.collection.mutable


//Todo 增加role data set weight
trait HeteroLinRParams extends PredictorParams with HasWeightCol with HasCoefficients
  with HasFitIntercept with HasRegParam with HasSingleLabelSideId with HasBatchFraction with
  FedParams with HasRegLambda with HasStepSize with HasMaxIter {

  // regParam:0.0 -> l2 1.0-> l1
  setDefault(regParam -> 0.0, batchFraction -> 0.1)

}


//   with HasElasticNetParam with HasMaxIter with HasTol
//  with HasFitIntercept with HasStandardization with HasWeightCol with HasSolver
//  with HasAggregationDepth with HasLoss with HasMaxBlockSizeInMB

/**
  *
  * @param V vec....
  */
//Todo 做成pipeline ，用flag->true/false 来fit，初始false，发现true 迭代
class HeteroLinR[V](override val uid: String) extends Regressor[Vector,
  HeteroLinR[V], HeteroLinRModel]
  with FederatedBase with HeteroLinRParams with HeteroBase
  with Logging {


  def this() = this(Identifiable.randomUID("HeteroLinR_siyuan"))


  //  setDefault (regParam -> 0.0, fitIntercept -> true, standardization -> true,
  //    elasticNetParam -> 0.0, maxIter -> 100, tol -> 1E-6, solver -> Auto,
  //    aggregationDepth -> 2, loss -> SquaredError, epsilon -> 1.35, maxBlockSizeInMB -> 0.0)

  //Todo 接口化

  var lossAggregator: FedDiffLossAggregator = _

  var optimizer: Gradient[V, DiffFunction[V]] = _

  var guestRefArray: Array[RpcEndpointRef] = _

  var keys:Array[Any] =_

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setCoefficients(value: Array[Double]): this.type = set(coefficients, value)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  def setFedLabel(value: Boolean): this.type = set(fedLabel, value)

  def setSingleLabelSideId(value: String): this.type = set(singleLabelSideId, value)

  def setRegParam(value: Double): this.type = set(regParam, value)

  def setRegLambda(value: Double): this.type = set(regLambda, value)

  def setStepSize(value: Double): this.type = set(stepSize, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setBatchFraction(value: Double): this.type = set(batchFraction, value)

  def setEncryptor(value: Encryptor): this.type = set(encryptor, value)

  def setPrimarykeyCol(value: String): this.type = set(primarykeyCol, value)

  def setJoinedkeys(keys:Array[Any]): Unit = this.keys =keys


  override def copy(extra: ParamMap): HeteroLinR[V] = {
    null
  }

  /**
    * Train a model using the given dataset and parameters.
    * Developers can implement this instead of `fit()` to avoid dealing with schema validation
    * and copying parameters into the model.
    *
    * @param dataset Training dataset
    * @return Fitted model
    */
  //Todo 做成pipeline ，用flag->true/false 来fit，初始false，发现true 迭代
  override protected def train(dataset: Dataset[_]): HeteroLinRModel = instrumented {

    instr =>

      null
  }


  //Todo 抽成公共接口
  def initialize(dataset: Dataset[_]): RDD[FedInstance] = instrumented {

    instr =>

      println("1")
      instr.logPipelineStage(this)
      println("2")

      instr.logDataset(dataset)
      println("3")

      //      instr.logParams(this, coefficients, fitIntercept, regParam, maxIter,batchFraction,
      //        fedLabel, stepSize, featuresCol, weightCol, predictionCol)
      //      println("4")

      val numFeatures = MetadataUtils.getNumFeatures(dataset, $(featuresCol))
      instr.logNumFeatures(numFeatures)

      println("numFeatures = " + numFeatures)

      println("$(isDefined(coefficients)", isDefined(coefficients))
      val initialCoef = Vectors.dense(if (!isDefined(coefficients)) {
//        Array.fill(numFeatures)(Utils.randomGaussian(0.1, 0.3, "0.0000"))
        Array.fill(numFeatures)(Utils.randomGaussian())
      } else $(coefficients))

      println("initialCoef = " + initialCoef)

      println("5")
      initializeLossAggregatorAndoptimizer(initialCoef)

      println("start extractFedInstances")
      val df = extractFedInstances(dataset)
      println("end extractFedInstances!!")

      df
  }

  def initializeLossAggregatorAndoptimizer(initialCoef: Vector): Unit = {
    //simulation data set or distribute

    println("FedLinRDiffLossAggregator")

    lossAggregator = new FedLinRDiffLossAggregator(initialCoef, getFitIntercept,
      getRegParam, getRegLambda, getStepSize, getMask, getEncryptor)

    println("MiniBatchGradientDescent")

//    optimizer = new MiniBatchGradientDescent[V](getMaxIter)

    optimizer = new HeteroMiniBatchGradientDescent[V](this.keys,getBatchFraction,getMaxIter)
  }


  //Todo map
  def setConfFromParamTool(paramTool: ParameterTool): Unit = {

    setFitIntercept(paramTool.get(Constants.ALCHEMY_PIPELINE_INTERCEPT).toBoolean)
    setRegParam(paramTool.get(Constants.ALCHEMY_PIPELINE_REGULARIZATION).toDouble)
    setRegLambda(paramTool.get(Constants.ALCHEMY_PIPELINE_SCALE_REGULARIZATION).toDouble)
    setStepSize(paramTool.get(Constants.ALCHEMY_PIPELINE_STEP_SIZE).toDouble)
    setFedLabel(paramTool.get(Constants.ALCHEMY_PIPELINE_FED_LABEL).toBoolean)
    setMaxIter(paramTool.get(Constants.ALCHEMY_PIPELINE_MAX_ITER).toInt)
    setBatchFraction(paramTool.get(Constants.ALCHEMY_PIPELINE_BATCH_FRACTION).toDouble)

  }


  //  def createModel(uid: String, coefficients: Vector): HeteroLinRModel ={
  //    new HeteroLinRModel(uid,coefficients)
  //  }
  override def fedFit(
                       //                       index: Long,epoch: Long,guestRefs: Array[RpcEndpointRef]
                     ): Unit = {

  }

}


class HeteroLinRModel private[ml](override val uid: String,
                                  val coefficients: Vector,
                                  val intercept: Double)
  extends RegressionModel[Vector, HeteroLinRModel] with GeneralMLWritable {

  private[ml] def this(uid: String, coefficients: Vector) =
    this(uid, coefficients, 0)

  /**
    * Predict label for the given features.
    * This method is used to implement `transform()` and output [[predictionCol]].
    */
  override def predict(features: Vector): Double = {
    dot(features, coefficients) + intercept
  }

  override def copy(extra: ParamMap): HeteroLinRModel = {
    val newModel = copyValues(new HeteroLinRModel(uid, coefficients, intercept), extra)
    //    newModel.setSummary(trainingSummary).setParent(parent)
    //Todo 改
    newModel
  }

  /**
    * Returns an `MLWriter` instance for this ML instance.
    */
  override def write: GeneralMLWriter = new GeneralMLWriter(this)
}


/** A writer for HeteroLinRModel that handles the "internal" (or default) format
  *
  * put path -> E:....\spark-master\mllib\src\main\resources\META-INF\services\org.apache.spark.ml.util.MLFormatRegister
  * */
private class InternalHeteroLinRModelWriter
  extends MLWriterFormat with MLFormatRegister {

  override def format(): String = "internal"

  override def stageName(): String = "org.apache.spark.ml.fed.model.hetero.linr.HeteroLinRModel"

  private case class Data(intercept: Double, coefficients: Vector)

  override def write(path: String, sparkSession: SparkSession,
                     optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    val instance = stage.asInstanceOf[HeteroLinRModel]
    val sc = sparkSession.sparkContext
    // Save metadata and Params
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    // Save model data: intercept, coefficients, scale
    val data = Data(instance.intercept, instance.coefficients)
    val dataPath = new Path(path, "data").toString
    sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
  }
}


object HeteroLinRModel extends MLReadable[HeteroLinRModel] {

  def createModel(uid: String, coefficients: Vector): HeteroLinRModel = {
    new HeteroLinRModel(uid, coefficients)
  }

  @Since("1.6.0")
  override def read: MLReader[HeteroLinRModel] = new HeteroLinRModelReader

  @Since("1.6.0")
  override def load(path: String): HeteroLinRModel = super.load(path)

  private class HeteroLinRModelReader extends MLReader[HeteroLinRModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[HeteroLinRModel].getName

    override def load(path: String): HeteroLinRModel = {
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
        new HeteroLinRModel(metadata.uid, coefficients, intercept)
      } else {
        // Spark 2.3 and later
        val Row(intercept: Double, coefficients: Vector) =
          data.select("intercept", "coefficients").head()
        new HeteroLinRModel(metadata.uid, coefficients, intercept)
      }

      metadata.getAndSetParams(model)
      model
    }
  }

}