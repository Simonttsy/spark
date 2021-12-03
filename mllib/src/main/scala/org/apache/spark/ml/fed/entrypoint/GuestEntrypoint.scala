package org.apache.spark.ml.fed.entrypoint

import org.apache.spark.ml.fed.culster.guest.GuestContext
import org.apache.spark.ml.fed.utils.ParameterTool

object GuestEntrypoint {


  def main(args: Array[String]): Unit = {

    // --guest-conf E:\projects\code\IdeaProjects\fed1\spark-master\mllib\src\test\resources\fed\guest\guest.properties
    //    val uri = Utils.convertToUri("t1", "192.168.146.1", "1053")

    val argsss = Array("--guest-conf E:\\projects\\code\\IdeaProjects\\fed1\\spark-master\\mllib\\src\\test\\resources\\fed\\guest\\guest.properties")


    val paramTool = ParameterTool.fromPropertiesFile(ParameterTool.fromArgs(args).get("guest-conf"))

    import scala.collection.JavaConverters._

    println(paramTool.toMap.asScala.mkString(","))

    new GuestContext(paramTool).run()

  }
}
