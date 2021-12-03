package org.apache.spark.ml.fed.culster.guest.driver.rpc

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.mapreduce.Job.JobState
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator}
import org.apache.spark.ml.fed.connector.Connector
import org.apache.spark.ml.fed.constants.Constants
import org.apache.spark.ml.fed.culster.guest.GuestTransformation
import org.apache.spark.ml.fed.encryptor.Masker
import org.apache.spark.ml.fed.encryptor.paillier.cli.SerialisationUtil
import org.apache.spark.ml.fed.event.ClusterMessages._
import org.apache.spark.ml.fed.event.PartyLossReason
import org.apache.spark.ml.fed.model.ModelMessages._
import org.apache.spark.ml.fed.model.base.{HeteroBase, HomoBase}
import org.apache.spark.ml.fed.model.evaluator.FedBinaryClassificationEvaluator
import org.apache.spark.ml.fed.model.hetero.linr.HeteroLinRMessages.HeteroLinRInitLabelSideVar
import org.apache.spark.ml.fed.model.hetero.linr.HeteroLinRModel
import org.apache.spark.ml.fed.model.homo.HomoLRModel
import org.apache.spark.ml.fed.model.loss.TaskDescription
import org.apache.spark.ml.fed.model.optimizer.Gradient.State
import org.apache.spark.ml.fed.scheduler.GuestData
import org.apache.spark.ml.fed.utils.Utils
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ThreadUtils

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
  *
  * @param arbiterUri
  * @param guestId
  * @tparam T original value type
  * @tparam C coded value type
  */
class GuestDriverRpcEndpoint[C: ClassTag](spark: SparkSession,
                                          arbiterUri: String,
                                          selfId: String,
                                          onArbiter: Boolean) extends ThreadSafeRpcEndpoint with Logging {

  @volatile var state: JobState = _

  @volatile var arbiterRef: Option[RpcEndpointRef] = None

  // coded values -> original values
  private val codedValuesMap = new ConcurrentHashMap[C, Any]

  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  var transformer: GuestTransformation[Vector] = _

  private var allGuestDataMap: ConcurrentHashMap[String, GuestData] = _

  private var selfData: GuestData = _

  private var allSamples: DataFrame = _

  private var modelSavePath: String = _

  private var aggrSideData: GuestData = _


  override def onStart(): Unit = {

    logInfo("Connecting to arbiter: " + arbiterUri)
    println("Connecting to arbiter: " + arbiterUri)
    println("guest on start id " + selfId)

    rpcEnv.asyncSetupEndpointRefByURI(arbiterUri).flatMap(ref => {

      arbiterRef = Some(ref)

      ref.ask[Boolean](RegisterGuest(selfId, self))

    })(ThreadUtils.sameThread).onComplete {
      case Success(_) =>
        println("ssssssssss")
        self.send(RegisteredGuest)
      case Failure(e) =>
        println("eeeeeeeeeeeee")
        exitBackend(1, s"Cannot register with arbiter: $arbiterUri", e, notifyArbiter = false)
    }(ThreadUtils.sameThread)
  }

  //Todo 每个模型有PartialFunction抽象出来放这里
  override def receive: PartialFunction[Any, Unit] = {
    case SubmitTask(task) =>

      println("SubmitTask - >" + task.id)

      if (transformer == null) {
        exitBackend(1, "Submit Task command but instance rdd of the transformer was null")
      } else {

        allGuestDataMap = task.guestDataMap.asInstanceOf[ConcurrentHashMap[String, GuestData]]

        aggrSideData = allGuestDataMap.get(task.aggrSideId)

        selfData = allGuestDataMap.get(selfId)

        val confMap = selfData.resourceRequest.parameterTool.toMap

        println("--------parameterTool--------")
        confMap.asScala.foreach(a => {
          println(a)
        })
        println("--------parameterTool--------")

        val dir = confMap.get(Constants.ALCHEMY_PIPELINE_MODEL_SAVE_DIR)
        val fileName = confMap.get(Constants.ALCHEMY_PIPELINE_MODEL_SAVE_FILE_NAME)

        println("-dir-fName-")
        println("dir", dir)
        println("fileName", fileName)

        modelSavePath = Utils.concatPath(selfId, dir, fileName)

        transformer.pipeline = selfData.pipeline

        transformer.encryptor = task.encryptor

        //        Utils.mapDeepCopy(otherGuestDataMap, task.guestDataMap)
        //        otherGuestDataMap.remove(selfData)

        println("createSourceConnector")

        //Todo 配置做到各自的guest上不通过task分发
        val source = Connector.createSourceConnector(confMap, spark)

        //transformation 初始化完毕后 un掉
        allSamples = source.read().cache()

        println("examples show 10")
        allSamples.show(10)

        transformer.pipeline.getStages.last match {
          case _: HomoBase =>
            println("HomoBase initialize(allSamples)")
            transformer.initialize(allSamples)
            arbiterRef.get.send(AlreadyFits(selfId))

          case _: HeteroBase =>
            println("HeteroBase initialize(allSamples)")
            transformer.userPrimaryKey = task.primaryKey

            println("userPrimaryKey = " + transformer.userPrimaryKey)
            val keysDF = allSamples.select(col(transformer.userPrimaryKey))

            //保持局部 不然就要bc
            val masker = task.joiner.asInstanceOf[Masker[C]]

            //主键必不能为空
            val codedMap = keysDF.rdd.map { case Row(pk: Any) => (masker.code(pk).get, pk) }.collect()

            //        val kValues = Seq(1, 2, 3, 4, 5, 6, 7).asInstanceOf[Array[T]]
            //
            //        val codeMap = joiner.toCodedMap(kValues)

            val codedKeys = for (elem <- codedMap) yield {
              println("guest", selfId, "codedMap", elem._1, elem._2)
              codedValuesMap.put(elem._1, elem._2)
              elem._1
            }

            //Todo 大小
            logInfo(s"guest_id_$selfId primary keys size is ${codedMap.length}")

            println("send(JoinKey) id+" + selfId)
            arbiterRef.get.send(JoinKey(selfId, codedKeys))
        }

      }

    case RegisteredGuest =>
      logInfo(s"Successfully registered self(id = $selfId) with driver")

      //      //Todo add fed params
      //      transformer = new GuestTransformation(selfId, null, null)
      //      transformer.initialize()

      transformer = new GuestTransformation[Vector]()

      println("send LaunchTask id  " + selfId)
      arbiterRef.get.send(LaunchTask(selfId))

    //      try {
    //        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false,
    //          resources = _resources)
    //        driver.get.send(LaunchedExecutor(executorId))
    //      } catch {
    //        case NonFatal(e) =>
    //          exitExecutor(1, "Unable to create due to " + e.getMessage, e)
    //      }


    case JoinComplete(joinedValues) =>

      println("id+" + selfId, "joinedValueslength =", joinedValues.length)

      if (joinedValues.nonEmpty) {

        val joinedDecodedValues = codedValuesMap.asScala.filter(map => {
          val coded = map._1
          joinedValues.contains(coded)
        }).values.toArray

        transformer.keys = joinedDecodedValues

        println("transformer.initialize")
        transformer.initialize(allSamples)


      } else exitBackend(2, s"Cannot complete join due to joined Values empty size:${joinedValues.length}")

      logInfo(s"guest_id_$selfId join key completed ,values size:${joinedValues.length}")


      println("send(JoinCompleteResponse,keylength = " + transformer.keys.length)

      arbiterRef.get.send(AlreadyFits(selfId))


    //Todo 统一接口
    case FitModel(epoch, blockIndex, shuffle, seed) =>
      println("receive fit model on " + selfId)
      println("epoch, blockIndex, shuffle, seed", epoch, blockIndex, shuffle, seed)

      transformer.optimizer.index = blockIndex

      if (shuffle) {
        transformer.optimizer.shuffleData(transformer.trainInstances, seed)
        transformer.optimizer.epoch = epoch
      }

      println("=-=-=当前 index ", transformer.optimizer.index, "transformer.optimizer.epoch", transformer.optimizer.epoch)


      val blockData = transformer.optimizer.extractBlockData(transformer.optimizer.index)

      //Todo 统一loss接口
      if (transformer.homoLRLossAggregator != null) {

        val (loss, grad) = transformer.homoLRLossAggregator.computeBinaryLossAndGrad(blockData, transformer.optimizer.batchSize)

        println("loss, grad", loss, grad)
        transformer.homoLRLossAggregator.reportLossAndGrad(loss, grad, aggrSideData.ref)

      } else if (transformer.lossAggregator != null) {


        logInfo("fitting model on side with label = " + selfData.hasLabel)

        println("selfData.hasLabel?" + selfData.hasLabel)

        if (!selfData.hasLabel) {

          //Todo 改成成array
          val labelSideRef = allGuestDataMap.asScala.filter(map => map._2.hasLabel).head._2.ref

          transformer.lossAggregator.initCommonSideLocalVar(blockData, labelSideRef)
        }
      }


    case HeteroLinRInitLabelSideVar(serialisedArray, serialisedEncryptedLoss) =>

      logInfo(s"HeteroLinRCommonSideLocalVar ,guest is $selfId and label is ${transformer.hasLabel}")

      println("HeteroLinRInitLabelSideVar--index", transformer.optimizer.index, "transformer.hasLabel?" + transformer.hasLabel)

      if (transformer.hasLabel) {

        //保障一次性不复用
        val blockData = transformer.optimizer.extractBlockData(transformer.optimizer.index)

        println("blockData-batchSize", transformer.optimizer.batchSize)


        val commonSideRefArray = allGuestDataMap.asScala.filter(map => {
          !map._2.hasLabel
        }).map(_._2.ref).toArray

        println("serialisedArray.size", serialisedArray.length)
        println("encryptedLoss", serialisedEncryptedLoss)
        println("commonSideRefArray.length", commonSideRefArray.length)
        println("arbiterRef.get =nul", arbiterRef.get == null)

        val key2EncryptedWDotXArray = serialisedArray.map(tuple2 => {
          val encrypted = SerialisationUtil.unserialiseEncrypted(tuple2._2, transformer.encryptor.getPublicKey)
          val key = tuple2._1
          (key, encrypted)
        })


        transformer.lossAggregator.initLabelSideVar(selfId,
          blockData,
          transformer.optimizer.batchSize,
          //          serialisedArray,
          key2EncryptedWDotXArray,
          SerialisationUtil.unserialiseEncrypted(serialisedEncryptedLoss, transformer.encryptor.getPublicKey),
          //          serialisedEncryptedLoss,
          commonSideRefArray,
          arbiterRef.get)
      }


    case HeteroLinRFullResidualArray(serKey2EncrFullResidualArray) =>

      if (!transformer.hasLabel) {
        val rdd = transformer.optimizer.curBatch

        val key2EncrFullResidualArray = serKey2EncrFullResidualArray.map(t => {

          (t._1, SerialisationUtil.unserialiseEncrypted(t._2, transformer.encryptor.getPublicKey))
        })

        transformer.lossAggregator.aggregateCommonSideCoef(selfId, rdd,
          transformer.optimizer.batchSize,
          key2EncrFullResidualArray,
          arbiterRef.get)
      }


    case HeteroLinRGuestDecryptedGrad(localMaskedGrad) =>
      println("receive decrypted gradient on guest witch id is", selfId)
      for (elem <- localMaskedGrad.map(_ - transformer.mask)) {
        println("receive decrypted gradient = ", elem, "id =", selfId)
      }

      //Todo checkpoint
      val coefVector = Vectors.dense(transformer.lossAggregator.updateCoef(localMaskedGrad))

      println("updateParams coefVector")

      transformer.lossAggregator.updateParams(coefVector)

      GuestDriverRpcEndpoint.this.synchronized {
        transformer.optimizer.stateBuf += State(coefVector, 0, transformer.optimizer.epoch)
      }

      println("+= State,transformer.optimizer.epoch", transformer.optimizer.epoch)

      println("unpersist")
      transformer.optimizer.unpersistCurBatch()

      println("send(CompleteLocalCoefUpdate")
      arbiterRef.get.send(CompleteLocalCoefUpdate(selfId))


    case HomoLRAvg(selfGuestId, serLoss, serGrad) =>
      println("HomoLRAvg 接收到 selfGuestId", selfGuestId)

      val pk = transformer.encryptor.getPublicKey
      val encrLoss = SerialisationUtil.unserialiseEncrypted(serLoss, pk)
      val encrGrad = serGrad.map(SerialisationUtil.unserialiseEncrypted(_, pk))

      println("+= TaskDescription ")

      GuestDriverRpcEndpoint.this.synchronized {
        transformer.homoLRLossAggregator.receivedTask += TaskDescription(selfGuestId, encrLoss, encrGrad)
      }
      println("receivedTask.size ", transformer.homoLRLossAggregator.receivedTask.size,
        "allGuestDataMap.size()", allGuestDataMap.size())

      if (transformer.homoLRLossAggregator.receivedTask.size == allGuestDataMap.size()) {
        println("=====receivedTask.size == allGuestDataMap.size=====")
        println("=====receivedTask.size == allGuestDataMap.size=====")

        val (avgLoss, avgGrad) = transformer.homoLRLossAggregator.averaging(allGuestDataMap.size())

        transformer.homoLRLossAggregator.receivedTask.clear()

        transformer.homoLRLossAggregator.decryptAvgStatistics(avgLoss, avgGrad, arbiterRef.get)
      }

    case HomoLRReportGrad(grad) =>
      println("HomoLRReportGrad grad -> ")
      grad.foreach(print(_))
      println("")

      val coefVector = transformer.homoLRLossAggregator.updateCoefficients(grad)

      GuestDriverRpcEndpoint.this.synchronized {
        transformer.optimizer.stateBuf += State(coefVector, 0, transformer.optimizer.epoch)
      }
      println("+= State.epoch", transformer.optimizer.epoch)

      transformer.optimizer.unpersistCurBatch()

      arbiterRef.get.send(CompleteLocalCoefUpdate(selfId))

    case Converged =>
      println("Converged_" + selfId)
      val stateView = transformer.optimizer.stateBuf.toArray
      val curState = stateView.last

      println("transformer.modelId", transformer.modelId)
      println("curState.w ->")
      curState.w.toArray.foreach(print)


      val evaluators: Array[Evaluator] = new Array[Evaluator](2)

      //Todo 统一接口
      val model = if (transformer.homoLRLossAggregator != null) {
        println("save HomoLRModel")

        evaluators(0) = new BinaryClassificationEvaluator()
          .setMetricName("areaUnderROC")

        evaluators(1) = new FedBinaryClassificationEvaluator()
          .setMetricName("ksTest")

        HomoLRModel
          .createModel(transformer.modelId, curState.w, transformer.homoLRLossAggregator.intercept)
        //          .write
        //          .session(spark)
        //          .save(modelSavePath + "_" + curTimestamp)

      } else {
        println("Save HeteroLinRModel")
        HeteroLinRModel
          .createModel(transformer.modelId, curState.w)
        //          .write
        //          .session(spark)
        //          .save(modelSavePath + "_" + curTimestamp)
      }

      /**
        * spark local ml
        * (coefficients,[0.02161872191285526,-0.004374831924905316,0.02643233647557198,
        * -0.04839744506258916,-0.08781978804775288,0.0668864383832009,0.04186613100384575,
        * -0.03460951888026342,-0.03583505917067532,-0.03779890798021467])
        */
      transformer.testDF.cache()
      val testResult = model.transform(transformer.testDF)
        .persist(StorageLevel.MEMORY_AND_DISK)
      println("show test result limit 50")
      testResult.show(50)

      transformer.testDF.registerTempTable("test_table")
      testResult.registerTempTable("t1")

      spark.sql("select count(1) from test_table where label == 1").show(false)
      spark.sql("select count(11) from t1 where prediction == 1").show(false)
      spark.sql("select count(0) from test_table where label == 0").show(false)
      spark.sql("select count(20) from t1 where prediction == 0").show(false)

      val auc = evaluators(0).evaluate(testResult)
      val ks = evaluators(1).evaluate(testResult)

      println("auc -> ", auc)
      println("ks -> ", ks)

      val curTimestamp = System.currentTimeMillis()

      println("save model " + modelSavePath + "_" + curTimestamp)

      model
        .write
        .session(spark)
        .save(modelSavePath + "_" + curTimestamp)

      testResult.unpersist()
      //      aucMetrics.unpersist()
      transformer.testDF.unpersist()

      println("send MetricsAvg")
      arbiterRef.get.send(MetricsAvg(selfId, Array(auc, ks)))

    case MetricsAvgCompleted(metrics) =>

      println("MetricsAvgCompleted id", selfId, "auc,ks ->", metrics.mkString(","))


  }

  //  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

  //    case HeteroLinRCommonSideLocalVar(key2EncryptedWDotXArray: Array[(C,EncryptedNumber)],encrytedLoss:EncryptedNumber)=>


  //  }

  //Todo arbiter 端删除该guest
  private def exitBackend(code: Int,
                          reason: String,
                          throwable: Throwable = null,
                          notifyArbiter: Boolean = true): Unit = {
    val message = "GuestBackend self-exiting due to : " + reason
    if (throwable != null) {
      logError(message, throwable)
    } else {
      logError(message)
    }

    if (notifyArbiter && arbiterRef.nonEmpty) {

      arbiterRef.get.send(RemoveParty(selfId, new PartyLossReason(reason)))
    }

    System.exit(code)
  }

  //    val arUrl = "spark://" + Utils.localHostNameForURI() + ":" + port
}

object GuestDriverRpcEndpoint {
  def driverEndpointName(id: String): String = "guest.endpoint.driver." + id
}