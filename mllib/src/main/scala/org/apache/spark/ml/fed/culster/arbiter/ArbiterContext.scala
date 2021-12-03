package org.apache.spark.ml.fed.culster.arbiter

import java.util

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.fed.SparkSessionBuilder
import org.apache.spark.ml.fed.constants.{Constants, RoleType}
import org.apache.spark.ml.fed.culster.resource.{ArbiterResourceRequest, GlobalResouceProfile, GuestResourceRequest}
import org.apache.spark.ml.fed.encryptor.paillier.{PaillierContext, PaillierPrivateKey}
import org.apache.spark.ml.fed.model.pipelinehelper._
import org.apache.spark.ml.fed.scheduler.{JobDescriptor, JobScheduler}
import org.apache.spark.ml.fed.utils.{ParameterTool, Preconditions, Utils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ArbiterContext(pts:java.util.List[ParameterTool]) {

  //Todo 这个类后面改成专门的arbiter的context构造，如果guest在本地的代码放在@onStart里！！！！！！！

  //Todo 后期增加 多fed 串行
  //Todo 检查重复主键，是否去重，不然后面计算要报错
  val jobCount = new java.util.concurrent.atomic.AtomicInteger(0)

  //Todo 指标,心跳,数据分布检测

  //Todo ip互通验证 2 用localHostName做一个ip生成器小工具，自动获取本机ip并发送到某个目标地

  //Todo 后期-所有的抽象构造器 做成对象放这里，真正执行放其他模块，比如scheduler

//  private val pts = ParameterTool.fromPropertiesDir(ParameterTool.fromArgs(args).get("arbiter-dir"))

  private val globalResourceProfile = GlobalResouceProfile(pts)

  private val arr: ArbiterResourceRequest = globalResourceProfile.arbiterResourceRequest

  private val grrMap: mutable.HashMap[String, GuestResourceRequest] = globalResourceProfile.guestResourceRequestMap

  private val pipelines: mutable.Buffer[FedPipeline] = createFedPipelineBuffer(pts)

  private val arbiterPipelineBuf = pipelines.filter(_.roleType == RoleType.ARBITER)

  Preconditions.checkBooleanCondition(arbiterPipelineBuf.size != 1, "arbiterPipeline is not unique,current size is " + arbiterPipelineBuf.size)

  private val arbiterRequest = ArbiterRequest(arr, arbiterPipelineBuf.head.pipeline)

  val privateKey: PaillierPrivateKey = PaillierPrivateKey.create(1024)
  val encryptor: PaillierContext = privateKey.getPublicKey.createSignedContext


  /**
    * 不包含arbiter
    */
  private val guestRequestMap: mutable.HashMap[String, GuestRequest] = zipFrom(pipelines, grrMap)

  private val spark = SparkSessionBuilder.builder().setConfMap(arr.sparkConfMap).build()

  private val jobId: String = JobDescriptor.JOB_ID_PREFIX + jobCount.getAndIncrement()

  def run(): Unit ={

    new JobScheduler(
      spark,
      arr.parameterTool.get(Constants.ALCHEMY_PRIMARY_KEY),
      arr.hostname,
      arr.port,
      JobDescriptor(jobId,
        privateKey,
        encryptor,
        arbiterRequest,
        guestRequestMap.toMap))

    Utils.getRpcEnv.awaitTermination()
  }

  private def createFedPipelineBuffer(pts: util.List[ParameterTool]): mutable.Buffer[FedPipeline] = {

    val buf = new ArrayBuffer[FedPipeline]()

    pts.forEach(paramTool => {

      buf += new DefaultPipelineBuilder(paramTool).build(paramTool.get(Constants.ALCHEMY_ROLE))

    })

    buf
  }

  private def zipFrom(pipelines: mutable.Buffer[FedPipeline],
                      grrMap: mutable.HashMap[String, GuestResourceRequest]): mutable.HashMap[String, GuestRequest] = {

    Preconditions.checkBooleanCondition(pipelines.length - 1 != grrMap.size,
      "guest pipelines can not match resource request number!!pipeline length is " + pipelines.length + ", grrMap size is " + grrMap.size)

    val map = new mutable.HashMap[String, GuestRequest]()

    pipelines
      .filter(_.roleType == RoleType.GUEST)
      .foreach(p => {
        val id = p.roleId
        val resource = grrMap(id)

        val onArbiter = resource.parameterTool.get(Constants.GUEST_ON_ARBITER).toBoolean

        map.put(id, GuestRequest(id, p.hasLabel,resource, p.pipeline,onArbiter))
      })
    map
  }
}

/**
  * Represents a fed model request task of the guest party
  *
  * @param id
  * @param resourceRequest
  * @param pipeline
  */
case class GuestRequest(id: String,
                        hasLabel: Boolean,
                        resourceRequest: GuestResourceRequest,
                        pipeline: Pipeline,
                        onArbiter:Boolean)

case class ArbiterRequest(resourceRequest: ArbiterResourceRequest,
                          pipeline: Pipeline)