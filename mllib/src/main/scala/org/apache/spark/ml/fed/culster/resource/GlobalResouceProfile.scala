package org.apache.spark.ml.fed.culster.resource

import org.apache.spark.ml.fed.constants.Constants
import org.apache.spark.ml.fed.culster.joiner.HashCodeJoiner
import org.apache.spark.ml.fed.encryptor.TestVarJoiner
import org.apache.spark.ml.fed.utils.{ParameterTool, Preconditions, Utils}

import scala.collection.mutable

case class GlobalResouceProfile(arbiterResourceRequest: ArbiterResourceRequest,
                                guestResourceRequestMap: mutable.HashMap[String, GuestResourceRequest])

//todo 增加远程拉起guest，确定guestdriverip，参考sparkcommend，参考flink，

object GlobalResouceProfile {

  def apply(pts: java.util.List[ParameterTool]): GlobalResouceProfile = {

    import scala.collection.JavaConversions._

    var arr: ArbiterResourceRequest = null

    val grrMap = mutable.HashMap[String, GuestResourceRequest]()

    //去重，统计是否guest唯一
    val tmpSet = new mutable.HashSet[String]()
    var arbiterNum = 0
    var guestNum = 0

    //Todo param参数检查 是否有有重复id等，arbiter和guest 只能重复一次
    pts.foreach(paramTool => {

      val role = paramTool.get(Constants.ALCHEMY_ROLE)

      val sparkConfMap = Utils.extractSparkConfMap(paramTool.toMap)

      val port = Preconditions.checkNotNone(sparkConfMap.get(Constants.SPARK_DRIVER_PORT), "Missing argument " + Constants.SPARK_DRIVER_PORT).get

      if (role.equalsIgnoreCase("arbiter")) {

        val joiner = if (paramTool.get(Constants.ALCHEMY_JOINER).equalsIgnoreCase("hash")) {
          HashCodeJoiner.builder()
            .setRandomRange(10, 99)
            .build()
        } else if (paramTool.get(Constants.ALCHEMY_JOINER).equalsIgnoreCase("var")) {
          new TestVarJoiner()
        } else throw new IllegalArgumentException("Unable to find this Joiner :" + paramTool.get(Constants.ALCHEMY_JOINER))

        arbiterNum += 1

        //Todo 做个抽象解析类 解析host port，可以手动传可以自动get本地
        val hostname = Utils.getHostname

        arr = ArbiterResourceRequest(hostname, port, joiner, paramTool, sparkConfMap)
      } else if (role.equalsIgnoreCase("guest")) {
        guestNum += 1

        val id = paramTool.get(Constants.GUEST_ID)
        tmpSet.add(id)
        val hostname = paramTool.get(Constants.GUEST_DRIVER_HOSTNAME)


//        val dir = paramTool.get(Constants.ALCHEMY_PIPELINE_MODEL_SAVE_DIR)
//        val fileName = paramTool.get(Constants.ALCHEMY_PIPELINE_MODEL_SAVE_FILE_NAME)
//        println("---")
//        println("dir",dir)
//        println("fileName",fileName)

        //Todo 做到Guest端判断下
        grrMap.put(id, GuestResourceRequest(id, hostname, port, paramTool, sparkConfMap))
      } else throw new IllegalArgumentException("alchemy.role must be arbiter or guest,current role error:" + role)

    })

    Preconditions.checkBooleanCondition(arbiterNum > 1, "arbiter just supported only one,current arbiter total number is:" + arbiterNum)

    Preconditions.checkBooleanCondition(tmpSet.size != guestNum, "guest id must be unique,current id total number is:" + guestNum)

    new GlobalResouceProfile(arr, grrMap)
  }
}
