package org.apache.spark.ml.fed.culster.joiner

/**
  *
  * @tparam T (converted) value type
  */

//Todo 增加多种编码 join的值类型
trait Joiner extends Serializable{

  //求key交集
  def innerJoin(thisSeq: Array[Any], otherSeq: Array[Any]): Array[Any]

}
