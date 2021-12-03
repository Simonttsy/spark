package org.apache.spark.ml.fed.connector

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

class JDBCConnector(spark:SparkSession,url:String,table:String,pros:Properties)  extends Connector {


  override def read(): DataFrame = {
    println("开始读jdbc")
    spark.read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver") //Todo 参数化
      .jdbc(url,table,pros)
  }
}
