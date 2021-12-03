package org.apache.spark.ml.fed.utils

import java.text.DecimalFormat
import java.util
import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.SparkEnv
import org.apache.spark.rpc.RpcEnv

import scala.collection.mutable
import scala.util.Random

//Todo private[fed]
object Utils {

  //Todo 增加映射名的支持
  def getHostname: String = org.apache.spark.util.Utils.localHostName()


  def convertToUri(endpointName: String, hostname: String, port: String): String = "spark://" + endpointName + "@" + hostname + ":" + port


  def extractSparkConfMap(map: util.Map[String, String]): mutable.Map[String, String] = {

    import scala.collection.JavaConversions._

    map.filter(_._1.startsWith("spark"))
  }

  def getRpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  def random(origin: Double, bound: Double): Double = {
    random(origin,bound,"0.000")
  }

  def random(origin: Double, bound: Double, format: String): Double = {
    //0.01, 0.09
    val d = ThreadLocalRandom.current().nextDouble(origin, bound)
    //0.0000
    new DecimalFormat(format).format(d).toDouble
  }

  def randomGaussian(origin: Double, bound: Double, format: String): Double ={

    randomNegative(random(origin, bound, format))
  }


  def randomGaussian(): Double ={
    val random = new Random()
    randomNegative((random.nextInt(9) + 1) * 0.01 + (random.nextInt(9) + 1) * 0.001)
  }



  def mapDeepCopy[K, V](copyTo:java.util.Map[K, V],map:java.util.Map[K, V]): Unit = copyTo.putAll(map)


  def defaultMask(): Double =Utils.random(0.001, 100.0)

  def concatPath(id:String,dir:String,fileName:String): String ={
    //Todo 正则判断dir和filename是否带‘/’符合,剪接
    dir + fileName+"_"+id
  }
  def randomNegative(value:Double): Double ={
    // 1 or 0
    if(Random.nextInt(2) == 0){
      -value
    }else value
  }
}
