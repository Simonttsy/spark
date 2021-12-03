package org.apache.spark.ml.fed.examples

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.ml.fed.event.ClusterMessages.RegisterGuest
import org.apache.spark.ml.fed.scheduler.JobScheduler
import org.apache.spark.ml.fed.utils.Utils
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.SparkSession

class JobSchedulerTest(spark: SparkSession,
                       arbiterDriverHostname: String,
                       arbiterDriverProt: String) {

  private val rpcEnv: RpcEnv = Utils.getRpcEnv

  var ref:RpcEndpointRef = _

  class ArbiterDriverRpcEndpoint extends ThreadSafeRpcEndpoint with Logging {
    override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv


    override def onStart(): Unit = {

      println("启动Arbiter")
//      println("arbiterDriverHostname",arbiterDriverHostname)
//      println("arbiterDriverProt",arbiterDriverProt)
//      GuestContext.createAndSetupEndpoint(spark, arbiterDriverHostname, arbiterDriverProt, "testId-01", onArbiter = true)

      println("启动Arbiter结束")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

      case RegisterGuest(guestId, guestRef) =>

        println("接收到guest注册了",guestId,guestRef.address)
    }

  }

  ref = rpcEnv.setupEndpoint(JobScheduler.ARBITER_DRIVER_ENDPOINT_NAME, createDriverEndpoint())
  println("ar address",ref.address)

  private def createDriverEndpoint(): ArbiterDriverRpcEndpoint = new ArbiterDriverRpcEndpoint
}
