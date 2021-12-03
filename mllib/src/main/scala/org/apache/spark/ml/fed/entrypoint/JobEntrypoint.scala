package org.apache.spark.ml.fed.entrypoint

import org.apache.spark.ml.fed.culster.arbiter.ArbiterContext
import org.apache.spark.ml.fed.culster.guest.GuestContext
import org.apache.spark.ml.fed.event.AlchemyJobEvent
import org.apache.spark.ml.fed.utils.{JsonUtil, ParameterTool}

object JobEntrypoint {

  def main(args: Array[String]): Unit = {

    run(args)
  }


  def run(args: Array[String]): Unit ={

    println("args:",args.mkString(","))
    println("args(0)",args(0))

    val jobEvent = JsonUtil.parseJsonTo(args(0),classOf[AlchemyJobEvent])

    println("jobEvent:",jobEvent.toString)

    if (jobEvent.getRoleType.equalsIgnoreCase("arbiter")){
      val pts =ParameterTool.fromPropertiesDir(jobEvent.getArbiterContextPath)
      new ArbiterContext(pts).run()
    }else if (jobEvent.getRoleType.equalsIgnoreCase("guest")){
      val pt = ParameterTool.fromPropertiesFile(jobEvent.getGuestContextPath)
      new GuestContext(pt).run()
    }else throw new IllegalArgumentException("RoleType is "+jobEvent.getRoleType)
  }
}
