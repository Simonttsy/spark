package org.apache.spark.ml.fed.examples

import org.apache.spark.sql.SparkSession

object FedMain {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("ml-test")
      .config("spark.local.dir", "E:\\tmp\\spark_tmp") // 生产注释掉
//      .config("spark.driver.port", "3344")
//      .enableHiveSupport()
      .getOrCreate()

    val instances = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15))


//    val df1 = spark.sparkContext.parallelize(Seq(F("1",1),F("2",2),F("3",3),F("4",4)))

//    val df2 = spark.sparkContext.parallelize(Seq(F("1",11),F("3",33),F("5",55),F("6",66)))

    val batchInstance = instances.sample(withReplacement = false, 0.3)

    batchInstance.foreachPartition(i=>{
      println("========")
      i.foreach(println(_))
    })
  }

  case class F(a:String,b:Double)
}
