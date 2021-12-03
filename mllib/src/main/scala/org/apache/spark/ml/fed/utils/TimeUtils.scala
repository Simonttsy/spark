package org.apache.spark.ml.fed.utils

import java.sql.Timestamp

object TimeUtils {

  def getCurrentTimestamp: String ={

    new Timestamp(System.currentTimeMillis()).toString
  }
}
