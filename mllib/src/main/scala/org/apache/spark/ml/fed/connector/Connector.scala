package org.apache.spark.ml.fed.connector

import java.util.Properties

import org.apache.spark.ml.fed.constants.Constants
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Connector {

  def read(): DataFrame

  //Todo 增加是否重复数据处理

}

object Connector {

  //Todo 配置做到各自的guest上不通过task分发
  def createSourceConnector(map: java.util.Map[String,String],
                            spark: SparkSession): Connector = {

    if (map.get(Constants.SOURCE_TYPE).equalsIgnoreCase("jdbc")) {
      val url = map.get(Constants.SOURCE_JDBC_URL)
      val table = map.get(Constants.SOURCE_JDBC_TABLE)
      val user = map.get(Constants.SOURCE_JDBC_USER)
      val password = map.get(Constants.SOURCE_JDBC_PASSWORD)

      val pros = new Properties()
      pros.put("user", user)
      pros.put("password", password)

      println(map.get(Constants.SOURCE_TYPE)," select from table",table)

     val jdbc =  new JDBCConnector(spark, url, table, pros)
      jdbc
    } else if(map.get(Constants.SOURCE_TYPE).equalsIgnoreCase("csv")){
      println("connecting csv")
      val path = map.get(Constants.SOURCE_CSV_PATH)
      new CSVConnector(spark,path)
    } else throw new IllegalArgumentException("Unable to find this Connector :" + map.get(Constants.SOURCE_TYPE))
  }
}
