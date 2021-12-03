package org.apache.spark.ml.fed

import org.apache.spark.internal.Logging
import org.apache.spark.ml.fed.constants.Constants
import org.apache.spark.ml.fed.utils.Preconditions
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SparkSessionBuilder extends Logging {

  private val ENABLE_HIVE_SUPPORT = "enable.hive.support"

  class Builder extends Logging {

    private[this] var options: mutable.Map[String, String] = _


    def setConfMap(confMap: mutable.Map[String, String]): Builder = synchronized {

      options = confMap
      this
    }

    def build(): SparkSession = synchronized {

      Preconditions.checkNotNull(options, "Unable to initialize from empty map")

//      val master = Preconditions.checkNotNone(options.get(Constants.SPARK_MASTER), "Missing argument " + Constants.SPARK_MASTER).get

      var sessionBuilder = SparkSession.builder()

      options.get(Constants.SPARK_MASTER) match {
        case Some(master) =>
          println("set master",Constants.SPARK_MASTER)
          sessionBuilder.master(master)
        case None =>
      }

      sessionBuilder.config(Constants.SPARK_DRIVER_PORT,options(Constants.SPARK_DRIVER_PORT))


//      var sessionBuilder = SparkSession.builder().master(master).config(Constants.SPARK_DRIVER_PORT,options(Constants.SPARK_DRIVER_PORT))

      if (options.getOrElse(ENABLE_HIVE_SUPPORT, "false").toBoolean) sessionBuilder = sessionBuilder.enableHiveSupport()

      val spark = sessionBuilder.getOrCreate()

      // Cannot modify the value of a Spark config: spark.driver.port by spark.conf.set
      for (elem <- options.filter(_._1!=Constants.SPARK_DRIVER_PORT)) spark.conf.set(elem._1, elem._2)

      spark.sparkContext.setLogLevel("WARN")

      spark
    }
  }

  def builder(): Builder = new Builder

}
