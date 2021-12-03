package org.apache.spark.ml.fed.examples

import org.apache.spark.SparkEnv
import org.apache.spark.ml.fed.utils.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.ThreadUtils

import scala.util.{Failure, Success}

object FedUtilTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("ml-test")
      .config("spark.local.dir", "E:\\tmp\\spark_tmp") // 生产注释掉
            .config("spark.driver.port", "3344")
      //      .enableHiveSupport()
      .getOrCreate()

    val rpcEnv = Utils.getRpcEnv

    val uri = Utils.convertToUri("guest.endpoint.driver", "DESKTOP-4IT56L1", "3344")

    println(1,uri)
    rpcEnv.setupEndpoint("arbiter.endpoint.driver",new TestFedEndpointArbiter(null))
    val driverEndpoint = rpcEnv.setupEndpoint("t1", new TestFedRpcEndpointGuest(uri))


  }

  def fit(url:String): Unit ={

    println(2,url)

    //Todo Spark 发送大量数据接口（blockmanager
    //Todo try 失败sleep段时间 重来几次
    val eventualRef = SparkEnv.get.rpcEnv.asyncSetupEndpointRefByURI(url)

    eventualRef.map(ref=>{

      println("=========执行ref====")
//      ref.send(Te("send data of party A"))
    })(ThreadUtils.sameThread)
      .onComplete {
        case Success(_) =>
          println("===========Success=========")
        case Failure(e) =>

          println("===========eeeeeeeee=========")
        //          exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
      }(ThreadUtils.sameThread)

    println(333,eventualRef)

  }

  def registerArbiterRpcEndpoint(partyB:String): Unit ={
    println("======registerArbiterRpcEndpoint=========",SparkEnv.get.rpcEnv.address)

    val endpointName="Fed-Entrypoint"

//    SparkEnv.get.rpcEnv.setupEndpoint(
//      endpointName, new FedRpcEndpoint(partyB))
//
//    // 不加 endpointName -> spark://sit-poc3.novalocal:46147
//    // spark://Fed-Entrypoint@sit-poc3.novalocal:46147
//    val url = "spark://"+endpointName+"@" + Utils.localHostNameForURI() + ":" + SparkEnv.get.rpcEnv.address.port
//
//    println(111,url)

  }
}
