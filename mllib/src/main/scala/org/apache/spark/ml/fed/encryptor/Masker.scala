package org.apache.spark.ml.fed.encryptor

/**
  *
  * @tparam T input type
  * @tparam C coded type
  */
trait Masker[C] extends Serializable {

  def code(arr:Array[Any]): Array[C]

  def code(i:Any):Option[C]

  def decode(arr:Array[C]): Array[Any]

  /**
    *
    * @param vs original values
    * @return coded value -> original value
    */
  def toCodedMap(vs:Array[Any]): Map[C,Any]
}
