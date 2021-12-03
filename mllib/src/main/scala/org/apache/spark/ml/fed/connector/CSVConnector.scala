package org.apache.spark.ml.fed.connector

import org.apache.spark.sql.{DataFrame, SparkSession}

class CSVConnector(spark:SparkSession,path:String) extends Connector{

  override def read(): DataFrame ={
//    val path:String = "E:\\tmp\\e_ml_examples\\train_10000.csv"
//    val path:String = "E:\\dataset\\credit2\\data_t492_f10000.csv"
    read(path,"true")
  }

  def read(path:String,header:String): DataFrame ={


    val df0 = spark
      .read
      .format("csv")
      .option("header", value = true)
      //      .option("multiLine", true)
      .load(path)
      .cache()

    val df = df0.selectExpr(
      "cast(V1 as double)",
      "cast(V2 as double)",
      "cast(V3 as double)",
      "cast(V4 as double)",
      "cast(V5 as double)",
      "cast(V6 as double)",
      "cast(V7 as double)",
      "cast(V8 as double)",
      "cast(V9 as double)",
      "cast(V10 as double)",
      "cast(Class as double) as label")

    df
//    //todo 增加动态参数
//    val df = spark
//      .read
//      .format("csv")
//      .option("header",header)
//      //      .option("multiLine", true)
//      .load(path)
//
//    //Todo 动态转换成double
//    df.selectExpr("id",
//      "cast(col1 as double)",
//      "cast(col2 as double)",
//      "cast(col3 as double)",
//      "cast(col4 as double)",
//      "cast(col5 as double)",
//      "cast(col6 as double)",
//      "cast(col7 as double)",
//      "cast(col8 as double)",
//      "cast(col9 as double)",
//      "cast(col10 as double)",
//      "cast(label as double)")
  }

  def test(path:String): Unit ={
    read(path,"true")
  }
}
