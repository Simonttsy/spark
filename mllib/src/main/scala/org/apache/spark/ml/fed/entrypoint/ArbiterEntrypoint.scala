package org.apache.spark.ml.fed.entrypoint

object ArbiterEntrypoint {

  def main(args: Array[String]): Unit = {


  }


  case class Arguments(
                        arbiterUrl: String,
                        guestId: String,
                        hostname: String,
                        port: String,
                        //                        cores: Int,
                        //                        appId: String,
                        //                        workerUrl: Option[String],
                        //                        userClassPath: mutable.ListBuffer[URL],
                        //                        resourcesFileOpt: Option[String],
                        resourceProfileId: Int)

}
