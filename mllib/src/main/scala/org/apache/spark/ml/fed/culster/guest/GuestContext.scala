package org.apache.spark.ml.fed.culster.guest

import org.apache.spark.internal.Logging
import org.apache.spark.ml.fed.SparkSessionBuilder
import org.apache.spark.ml.fed.constants.Constants
import org.apache.spark.ml.fed.culster.guest.driver.rpc.GuestDriverRpcEndpoint
import org.apache.spark.ml.fed.culster.resource.GuestResourceRequest
import org.apache.spark.ml.fed.scheduler.JobScheduler
import org.apache.spark.ml.fed.utils.{ParameterTool, Preconditions, Utils}
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class GuestContext[T: ClassTag, C: ClassTag](paramTool:ParameterTool) {

  private val resourceRequest: GuestResourceRequest = generateResourceRequest()

  val spark: SparkSession = SparkSessionBuilder.builder().setConfMap(resourceRequest.sparkConfMap).build()

  val arbiterDriverHostname: String = paramTool.get(Constants.ARBITER_DRIVER_HOSTNAME)
  val arbiterDriverPort: String = paramTool.get(Constants.ARBITER_DRIVER_PORT)

  def run(): Unit ={
    GuestContext.createAndSetupEndpoint(spark,arbiterDriverHostname,arbiterDriverPort,resourceRequest.guestId,onArbiter = false)

    Utils.getRpcEnv.awaitTermination()
  }

  def generateResourceRequest(): GuestResourceRequest ={

    val sparkConfMap = Utils.extractSparkConfMap(paramTool.toMap)

    val port = Preconditions.checkNotNone(sparkConfMap.get(Constants.SPARK_DRIVER_PORT), "Missing argument " + Constants.SPARK_DRIVER_PORT).get

    val selfId = paramTool.get(Constants.GUEST_ID)
    val selfHostname = paramTool.get(Constants.GUEST_DRIVER_HOSTNAME)
    //Todo 做到Guest端判断下
    GuestResourceRequest(selfId, selfHostname, port, paramTool, sparkConfMap)

  }

}

object GuestContext extends Logging{

  val GUEST_DRIVER_ENDPOINT_NAME: String = "guest.endpoint.driver"


  def createDriverEndpoint[C: ClassTag](spark:SparkSession,hostname:String,port: String,selfId:String,onArbiter:Boolean):GuestDriverRpcEndpoint[C] = {

    val arDriverUri = Utils.convertToUri(JobScheduler.ARBITER_DRIVER_ENDPOINT_NAME,hostname,port)

    new GuestDriverRpcEndpoint[C](spark,arDriverUri,selfId,onArbiter)
  }

  def createAndSetupEndpoint(spark:SparkSession,hostname:String,port: String,selfId:String,onArbiter:Boolean): Unit ={
    logInfo("initialize guest rpc witch id is "+selfId)
    val rpcEndpoint = createDriverEndpoint(spark,hostname,port,selfId,onArbiter)
    println("注册")
    Utils.getRpcEnv.setupEndpoint(GuestContext.GUEST_DRIVER_ENDPOINT_NAME+".selfId", rpcEndpoint)
  }
}
