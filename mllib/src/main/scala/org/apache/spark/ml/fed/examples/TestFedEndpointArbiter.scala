package org.apache.spark.ml.fed.examples

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkEnv
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.fed.culster.joiner.HashCodeJoiner
import org.apache.spark.ml.fed.culster.resource.GuestResourceRequest
import org.apache.spark.ml.fed.encryptor.TestVarJoiner
import org.apache.spark.ml.fed.encryptor.paillier.cli.SerialisationUtil
import org.apache.spark.ml.fed.encryptor.paillier.{EncryptedNumber, PaillierContext, PaillierPrivateKey, PrivateKey}
import org.apache.spark.ml.fed.event.ClusterMessages.{LaunchTask, RegisterGuest}
import org.apache.spark.ml.fed.scheduler.GuestData
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class TestFedEndpointArbiter(spark: SparkSession) extends ThreadSafeRpcEndpoint {

  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  var gRef: RpcEndpointRef = _


  val privateKey: PaillierPrivateKey = PaillierPrivateKey.create(1024)
  var publicKey = privateKey.getPublicKey
  val paillierContext = publicKey.createSignedContext

  private val nuuu: EncryptedNumber = paillierContext.encrypt(6.16)

  val guestDataMap = new ConcurrentHashMap[String, GuestData]

  val grrMap = mutable.HashMap[String, String]()
  grrMap.put("1m", "1")
  grrMap.put("22", "22")

  //   val p = new ParameterTool()
  val request = GuestResourceRequest("1", "1", "1", null, grrMap)

  val pipeline = new Pipeline()

  guestDataMap.put("1", GuestData("1", hasLabel = false, request, pipeline, self))
  guestDataMap.put("2", GuestData("2", hasLabel = false, request, pipeline, self))
  guestDataMap.put("3", GuestData("3", hasLabel = false, request, pipeline, self))


  val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))


  val joiner = if (true) {
    HashCodeJoiner.builder()
      .setRandomRange(10, 99)
      .build()
  } else if (false) {
    new TestVarJoiner()
  } else throw new IllegalArgumentException("Unable to find this Joiner :")


  override def onStart(): Unit = {
    println("on start le")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {


    case RegisterGuest(i, s) =>

      println("RegisterGuest")
      context.reply(paillierContext)


    case TestEncryptor(e) =>

      //      println("HeteroLinRInitLabelSideVar")
      //      println("e ", e)
      //
      //      val a1 = e.encrypt(2.2)
      //      val a2 = e.encrypt(4.55)
      //
      //      println("a1 =" + a1)
      //      println("a1 +a2 =" + a1.add(a2))
      //      println("encryptedLoss == nul ", encryptedLoss == null)
      //      println(encryptedLoss.getClass.getName)
      //
      //      val sss = SerialisationUtil.unserialiseEncrypted(encryptedLoss,e.getPublicKey)
      //      println("sss"+sss)
      //      val number = a2.add(sss)
      //
      //      println("number " + privateKey.decrypt(number).decodeDouble())
      //      println("end")
      context.reply(true)
  }

  override def receive: PartialFunction[Any, Unit] = {


    case LaunchTask(id) =>

      println(1212, id)

      println(222, guestDataMap.size())
      gRef.send(Rs(privateKey))
      println(3, guestDataMap.size())

    //      gRef.send(SubmitTask(new TaskDescriptor("A", "CODE", privateKey, joiner, guestDataMap)))
    //      println(4, guestDataMap.size())


    case R(s, r) =>
      gRef = r
      println(11111, s)
      r.send("send(PP(paillierContext")
      gRef.send(PP(paillierContext))

    case SendEncrypted(e) =>

      val ff = SerialisationUtil.unserialiseEncrypted(e, paillierContext.getPublicKey)
      val bcvalue = rdd.context.broadcast(ff)
      val bcpaillierContext = rdd.context.broadcast(paillierContext)

      println("receive SendEncrypted")

      rdd.foreach(a => {

        val v = bcvalue.value
        val en = bcpaillierContext.value
        val ff = en.encrypt(a.toDouble).add(v)
        println("ff " + ff)
      })

      val ggg = nuuu.add(ff)
      val d = privateKey.decrypt(ggg).decodeDouble()

      println("dddd", d)
  }

}

case class R(s: String, r: RpcEndpointRef)

case class PP(paillierContext: PaillierContext)

//case class Rj(j: Joiner)  ok

case class Rg(guestDataMap: ConcurrentHashMap[String, GuestData])

case class Rs(privateKey: PrivateKey)

//case class Rss(s: Test)


//case class Test(id: String,
//                primaryKey: String,
//                privateKey: PrivateKey) extends Serializable {

