package org.apache.spark.ml.fed.scheduler

import java.lang
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.fed.culster.TaskDescriptor
import org.apache.spark.ml.fed.culster.arbiter.{ArbiterRequest, ArbiterTransformation, GuestRequest}
import org.apache.spark.ml.fed.culster.guest.GuestContext
import org.apache.spark.ml.fed.culster.joiner.Joiner
import org.apache.spark.ml.fed.culster.resource.GuestResourceRequest
import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.encryptor.paillier.PrivateKey
import org.apache.spark.ml.fed.encryptor.paillier.cli.SerialisationUtil
import org.apache.spark.ml.fed.event.ClusterMessages._
import org.apache.spark.ml.fed.event.PartyLossReason
import org.apache.spark.ml.fed.model.ModelMessages._
import org.apache.spark.ml.fed.model.base.{HeteroBase, HomoBase}
import org.apache.spark.ml.fed.model.optimizer.Gradient.State
import org.apache.spark.ml.fed.model.optimizer.HeteroMiniBatchGradientDescent
import org.apache.spark.ml.fed.utils.{Preconditions, Utils}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.ThreadUtils

import scala.collection.JavaConverters._


/**
  * fed 由pipeline组成
  *
  *
  * 1 :
  * job1 -> job2 -> job...
  *
  * 2:
  * job1           job4
  * -> job3 ->
  * job2           job5
  *
  *
  * fed -> fed
  */

//Todo 增加多fed调度，类似job图


// Todo 架构重新做！，JobScheduler划分好所有guest的调度pipeline，不需要发送pipelinestage过去
// TOdo 任务依次由 PartyA依次调度

class JobScheduler(spark: SparkSession,
                   primaryKey: String,
                   arbiterDriverHostname: String,
                   arbiterDriverProt: String,
                   job: JobDescriptor) extends Logging {
  //Todo 增加移除表MAP
  //Todo heartbeat,提交资源参数，模型参数,
  //Todo 加序列化支持

  private val rpcEnv: RpcEnv = Utils.getRpcEnv

  @volatile private var joinedCount: Int = 0
  @volatile private var alreadyFitsCount: Int = 0

  private val guestDataMap = new ConcurrentHashMap[String, GuestData]
  //all submitted task map
  //Todo 考虑容错问题 runningTaskMap 可能会退出-1
  //Todo 定时调度器 每一段时间查看这个值是否等于已经注册的值（类似心跳检测
  private val runningTaskMap: ConcurrentHashMap.KeySetView[String, lang.Boolean] = ConcurrentHashMap.newKeySet[String]()

  //  //guest_id -> Map(coded_value -> original_value)
  //  private val guest2CodeMap = new ConcurrentHashMap[String, Map[C, T]]

  private var joinedValues: Array[Any] = _

  //Todo 定时调度器 每一段时间查看这个值是否等于已经注册的值（类似心跳检测
  private val reviveThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(JobScheduler.REREVIVE_THREAD_NAME)


  private val privateKey: PrivateKey = job.privateKey

  private val jobId: String = job.jobId

  private var selfGuestId: String = _

  private val aRequest = job.aRequest
  private val gRequestMap: Map[String, GuestRequest] = job.gRequestMap

  private val guestNumber = gRequestMap.size

  private val joiner: Joiner = aRequest.resourceRequest.joiner

  var transformer: ArbiterTransformation[Vector] = _

  var encryptor: Encryptor = job.encryptor


  class ArbiterDriverRpcEndpoint extends ThreadSafeRpcEndpoint with Logging {

    override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

    // 被动发现Map有待激活的就launch，不会主动去launch
    override def onStart(): Unit = {

      val reviveIntervalMs = 2000L

      // guest and arbiter maybe deployed on same host

      val onArbiter = gRequestMap.filter(_._2.onArbiter)

      //Todo 增加guestContext的初始化
      if (onArbiter.size == 1) {
        val selfGuest = onArbiter.head._2

        selfGuestId = selfGuest.id

        println("guest on arbiter --- " + selfGuestId)
        logInfo(s"Registered guest ${selfGuest.resourceRequest.hostname}:${selfGuest.resourceRequest.port} on arbiter with ID ${selfGuest.id}")

        GuestContext.createAndSetupEndpoint(spark, arbiterDriverHostname, arbiterDriverProt, selfGuest.id, onArbiter = true)

      } else if (onArbiter.size > 1) {
        throw new IllegalArgumentException("onArbiter.size > 1 ,is " + onArbiter.size)
      }

      if (transformer == null) {
        //        println("gRequestMap",gRequestMap.size,"selfGuestId",selfGuestId)
        val guestIdWithoutArbiter = gRequestMap.keys.filter(_ != selfGuestId).toArray

        //        gRequestMap.keys.foreach(println("k",_))
        //
        //        gRequestMap.foreach(a=>println("gRequestMap",a))

        //        println("guestIdWithoutArbiter.length",guestIdWithoutArbiter.length)
        transformer = new ArbiterTransformation(aRequest.pipeline, guestIdWithoutArbiter)
        transformer.initialize(encryptor)

      }

      //Todo 增加重启逻辑
      //org.apache.spark.SparkException: Unsupported message OneWayMessage(sit-poc2.novalocal:38408,ReviveTask) from sit-poc2.novalocal:38408

//      reviveThread.scheduleAtFixedRate(() => org.apache.spark.util.Utils.tryLogNonFatalError {
//        Option(self).foreach(_.send(ReviveTask))
//      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)

    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {


      case RegisterGuest(guestId, guestRef) =>

        //Todo 增加移除表并判断

        if (guestDataMap.contains(guestId)) {
          context.sendFailure(new IllegalStateException(s"Duplicate guest ID: $guestId"))
        } else {

          logInfo(s"Registered guest $guestRef (${guestRef.address}) with ID $guestId")

          println(s"Registered guest $guestRef (${guestRef.address}) with ID $guestId")

          JobScheduler.this.synchronized {
            println("1111111")

            val gRequest = gRequestMap.get(guestId) match {
              case Some(value) => value
              case None =>
                println("NoneNoneNoneNone")
                throw new NullPointerException(s"can not find guest id $guestId in gRequestMap")
            }


            guestDataMap.put(guestId, GuestData(guestId, gRequest.hasLabel, gRequest.resourceRequest, gRequest.pipeline, guestRef))
            println("guestDataMap.siz = " + guestDataMap.size())

          }

          println("context.reply id " + guestId, context.senderAddress)
          context.reply(true)
        }

    }


    override def receive: PartialFunction[Any, Unit] = {


      case LaunchTask(id) =>

        Preconditions.checkBooleanCondition(runningTaskMap.contains(id), "Duplicate running guest id:" + id)

        runningTaskMap.add(id)

        logDebug(s"receive running guest id: $id")

        println(s"receive running guest id: $id", "guestNumber=", guestNumber, "runningTaskMap.size()=", runningTaskMap.size())

        //Todo 考虑容错问题 runningTaskMap 可能会退出-1
        if (guestNumber == runningTaskMap.size()) {

          //          Preconditions.checkBooleanCondition(guestNumber <= 1, "guest number =" + guestNumber)

          println("都注册进来了,guestDataMap.siz =" + guestDataMap)
          guestDataMap.asScala.foreach(a => println(a._1, a._2.id))
          guestDataMap.asScala.foreach {
            case (guestId, data) =>

              //Todo 参考spark 转byte
              //Todo 加序列化支持,增加发送大小阈值
              logDebug(s"Launching guest task , id: $guestId  address: ${data.ref.address}.")

              println("guestId", guestId, "primaryKey", primaryKey, "encryptor", encryptor,
                "joiner", joiner, "guestDataMap.size", "transformer.aggrSideId", transformer.aggrSideId, guestDataMap.size())

              println("SubmitTask +" + guestId, "data.ref address =", data.ref.address)

              //Todo 统一
              data.ref.send(SubmitTask(new TaskDescriptor(guestId, primaryKey, encryptor, joiner, transformer.aggrSideId, guestDataMap)))
          }
        }

      case JoinKey(id, guestValues) =>

        JobScheduler.this.synchronized {

          if (joinedValues == null) {
            println("joinedCount = null")
            joinedValues = guestValues
          } else {
            println("joiner.innerJoin(joinedValues, guestValues")
            joinedValues = joiner.innerJoin(joinedValues, guestValues)
          }
        }

        joinedCount += 1
        println("joinedCount =" + joinedCount, "guestDataMap.size()=" + guestDataMap.size())
        if (guestDataMap.size() == joinedCount) {
          logInfo(s"current joined key count is :$joinedCount,send values to all guest")

          println("send(JoinComplete")
          guestDataMap.asScala.foreach(_._2.ref.send(JoinComplete(joinedValues)))

        } else {
          logInfo(s"current joined count is :$joinedCount,remaining ${guestDataMap.size() - joinedCount} guest are waiting to joined")
        }

      case AlreadyFits(guestId) =>
        logInfo(s"receive guest Already Fits response witch id is " + guestId)
        alreadyFitsCount += 1

        println("AlreadyFits id ", guestId, "count", alreadyFitsCount)
        if (guestDataMap.size() == alreadyFitsCount) {
          logInfo(s"received all guest already fits ,start submit model task")

          println("SubmitModelTask")
          self.send(SubmitModelTask)
        }

      case SubmitModelTask =>

        //        Preconditions.checkBooleanCondition(transformer != null,"The transformer has been initialized on arbiter")
        //开始初始化Transformation

        //        transformer = new ArbiterTransformation(aRequest.pipeline, otherGuestRefs)
        //        transformer.initialize(encryptor)

        //Todo 代码统一到optimizer里
        transformer.pipeline.getStages.last match {
          case _: HeteroBase =>

            println("generateUnorderedBlockMeta,joinedValues->size", joinedValues.length)
            joinedValues.foreach(a => println("joinedValue", a))
            println("transformer.fraction = " + transformer.fraction)
            //Todo 优化成 guest一端计算好blockNumber发送给arbiter 再开始fit model
            //just to compute block meta size
            val blockMeta = transformer
              .optimizer
              .asInstanceOf[HeteroMiniBatchGradientDescent[Vector]]
              .generateUnorderedBlockMeta(joinedValues, transformer.fraction, transformer.seed)

            transformer.optimizer.blockNumber = blockMeta.keys.size

          case _: HomoBase => // do nothing
        }

        println("transformer.optimizer.blockNumber =" + transformer.optimizer.blockNumber)
        println("send FitModel")
        guestDataMap.asScala.foreach(_._2.ref.send(FitModel(transformer.optimizer.epoch,
          transformer.optimizer.index, shuffle = true, transformer.seed)))

      //Todo 统一arbiter decrypt接口 loss，grad
      case HeteroLinRFullLossAndGrad(guestId, serFullLoss, serMaskedGrad) =>

        println("2222222 HeteroLinRFullLossAndGrad 22222222222222")

        val fullLoss = SerialisationUtil.unserialiseEncrypted(serFullLoss, encryptor.getPublicKey)

        logInfo(s"Handle completed Hetero LinR loss ${encryptor.decryptToDouble(fullLoss, privateKey)} on guest witch id is " + guestId)


        JobScheduler.this.synchronized {
          transformer.optimizer.stateBuf += State(null, encryptor.decryptToDouble(fullLoss, privateKey), transformer.optimizer.epoch)
        }

        val localGrad = serMaskedGrad.map(mpd => encryptor.decryptToDouble(SerialisationUtil.unserialiseEncrypted(mpd, encryptor.getPublicKey), privateKey))

        guestDataMap.get(guestId).ref.send(HeteroLinRGuestDecryptedGrad(localGrad))

      //Todo 统一arbiter decrypt接口
      case HeteroLinRGuestMaskedGrad(guestId, maskedGrad) =>
        logInfo(s"Decrypting guest masked grad on arbiter that guest id is $guestId")

        println("HeteroLinRGuestMaskedGrad")

        val localGrad = maskedGrad.map(mpd => encryptor.decryptToDouble(SerialisationUtil.unserialiseEncrypted(mpd, encryptor.getPublicKey), privateKey))

        println("send(HeteroLinRGuestDecryptedGrad")
        guestDataMap.get(guestId).ref.send(HeteroLinRGuestDecryptedGrad(localGrad))

      //Todo 统一arbiter decrypt接口
      case HomoLRDecryptStatistics(serLoss, serGrad) =>
        println("HomoDecryptStatistics")

        val loss = SerialisationUtil.unserialiseEncrypted(serLoss, encryptor.getPublicKey)

        logInfo(s"Handle Homo LR loss ${encryptor.decryptToDouble(loss, privateKey)}")

        JobScheduler.this.synchronized {
          transformer.optimizer.stateBuf += State(null, encryptor.decryptToDouble(loss, privateKey), transformer.optimizer.epoch)
        }

        val grad = serGrad.map(pd => encryptor.decryptToDouble(SerialisationUtil.unserialiseEncrypted(pd, encryptor.getPublicKey), privateKey))

        guestDataMap.asScala.foreach(_._2.ref.send(HomoLRReportGrad(grad)))


      case CompleteLocalCoefUpdate(id) =>

        println("receive CompleteLocalCoefUpdate " + id)

        JobScheduler.this.synchronized {
          transformer.completedTasks += id
        }

        transformer.completedTasks.foreach(a => {
          println("----transformer.completedTasks---id", a)
        })

        println("transformer.completedTasks.size", transformer.completedTasks.size, "guestDataMap.size()", guestDataMap.size())

        logInfo("Receive complete guest coef updated witch id is " + id)

        if (transformer.completedTasks.size == guestDataMap.size()) {
          logInfo("All guest updated coef  , Start the next iteration")
          println("All guest updated coef  , Start the next iteration!!!!!!!!!")

          //Todo 改成结尾调用pipeline 的一次iter!!!!!!!!!!!!

          transformer.completedTasks.clear()

          //Todo convergence
          var shuffle: Boolean = false
          if (transformer.optimizer.index != transformer.optimizer.blockNumber) {
            //The current batch is not finished
            transformer.optimizer.index += 1

          } else {
            //finished one round of batch
            transformer.optimizer.epoch += 1
            shuffle = true
            transformer.optimizer.index = 1
          }

          println("transformer.optimizer.blockNumber", transformer.optimizer.blockNumber)
          println("transformer.optimizer.epoch", transformer.optimizer.epoch)
          println("transformer.optimizer.index", transformer.optimizer.index)

          transformer.optimizer.convergence(transformer.optimizer.stateBuf.last) match {
            case Some(_) =>
              println("Stop iteration")
              println("Stop iteration")
              println("transformer.optimizer.stateBuf.size", transformer.optimizer.stateBuf.size)
              transformer.optimizer.stateBuf.foreach(a => {
                println("a.epoch,loss", a.epoch, a.value)
              })

              guestDataMap.asScala.foreach(_._2.ref.send(Converged))

              //Todo 增加耗时，迭代次数等的统计
              logInfo(s"Stop iteration")
            case None =>
              println("next round batch!!")
              println("next round batch!!")
              transformer.updateSeed()

              println("transformer.optimizer.epoch, transformer.optimizer.index, shuffle, transformer.seed", transformer.optimizer.epoch, transformer.optimizer.index, shuffle, transformer.seed)
              guestDataMap.asScala.foreach(_._2.ref.send(FitModel(transformer.optimizer.epoch, transformer.optimizer.index, shuffle, transformer.seed)))
          }
        }

      case MetricsAvg(id, metrics) =>

        println("receive CommonAvg id " + id)
        println("transformer.completedTasks.length", transformer.completedTasks.length)

        JobScheduler.this.synchronized {
          transformer.completedTasks += id
          if (transformer.metrics != null) {
            transformer.metrics = transformer.metrics.zip(metrics).map {
              case (thisMetric, otherMetric) =>
                thisMetric + otherMetric
            }
          } else {
            transformer.metrics = metrics
          }

          println("arbiter metrics ->", transformer.metrics.mkString(","))
        }

        val taskSize = guestDataMap.size()

        println("completedTasksSize,GuestSize", transformer.completedTasks.size, taskSize)

        if (transformer.completedTasks.size == taskSize) {

          val avgMetrics = transformer.metrics.map(_ / taskSize.toDouble)

          println("arbiter avg metrics ->", avgMetrics.mkString(","))

          transformer.metrics = null
          transformer.completedTasks.clear()
          guestDataMap.asScala.foreach(_._2.ref.send(MetricsAvgCompleted(avgMetrics)))
        }

      //      case
      case RemoveParty(guestId: String, reason: PartyLossReason) =>

      //Todo remove

    }
  }


  rpcEnv.setupEndpoint(JobScheduler.ARBITER_DRIVER_ENDPOINT_NAME, createDriverEndpoint())

  private def createDriverEndpoint(): ArbiterDriverRpcEndpoint = new ArbiterDriverRpcEndpoint


}


object JobScheduler {
  val ARBITER_DRIVER_ENDPOINT_NAME: String = "arbiter.endpoint.driver"

  val REREVIVE_THREAD_NAME: String = "arbiter-driver-revive-thread"
}


case class GuestData(id: String,
                     hasLabel: Boolean,
                     resourceRequest: GuestResourceRequest,
                     pipeline: Pipeline,
                     ref: RpcEndpointRef)

/**
  * Represents a fed assignment
  *
  * @param jobId
  * @param aRequest
  * @param gRequestMap
  */
case class JobDescriptor(jobId: String,
                         privateKey: PrivateKey,
                         encryptor: Encryptor,
                         aRequest: ArbiterRequest,
                         gRequestMap: Map[String, GuestRequest])

object JobDescriptor {
  val JOB_ID_PREFIX: String = "fed-job-"
}