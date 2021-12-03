package org.apache.spark.ml.fed.examples

import org.apache.spark.SparkEnv
import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.encryptor.paillier.cli.SerialisationUtil
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.ThreadUtils

class TestFedRpcEndpointGuest(arbiterUri: String) extends ThreadSafeRpcEndpoint {

  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  var arbiterRef :Option[RpcEndpointRef] = _

  var paillierContext:Encryptor =_

  override def onStart(): Unit = {

    println("onstart=======register guest============")

    rpcEnv.asyncSetupEndpointRefByURI(arbiterUri).map(ref => {

      arbiterRef = Some(ref)

      println("send(R(\"1")
      ref.send(R("1",self))

    })(ThreadUtils.sameThread)

//    rpcEnv.asyncSetupEndpointRefByURI(arbiterUri).flatMap(ref => {
//
//      arbiterRef = Some(ref)
//
//      ref.ask[Boolean](RegisterGuest("111", self))
//
//    })(ThreadUtils.sameThread).onComplete {
//      case Success(_) =>
//        println("ssssssssss")
//        self.send(RegisteredGuest)
//      case Failure(e) =>
//        println("eeeeeeeeeeeee")
//    }(ThreadUtils.sameThread)
  }

  override def receive: PartialFunction[Any, Unit] = {

    case a: String =>
      println(22222,a)
    case PP(p)=>
      paillierContext = p
      val number = paillierContext.encrypt(2.345)
      val encrypted = SerialisationUtil.serialiseEncrypted(number)
      arbiterRef.get.send(SendEncrypted(encrypted))

    case _ => println("erorr.....")

  }

}

case class TestEncryptor(string: String)

case class SendEncrypted(s: SerialisationUtil.SerialisedEncrypted)