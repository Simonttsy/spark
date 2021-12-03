package org.apache.spark.ml.fed.model.loss

trait DiffFunction[V] {

//  var instances:DATA

  /**
    * Calculates both the value and the gradient at a point
    * @param x ????
    * @return
    */

  //iteration epoch,batch gradient in calculate
  def calculate(w:V): (Double,V)

//  /**
//    * 初始化或更新数据
//    * @param x instances
//    */
//  def replace(x:DATA): Unit = this.instances = x

}
