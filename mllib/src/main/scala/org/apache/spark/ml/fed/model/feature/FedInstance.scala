package org.apache.spark.ml.fed.model.feature

import org.apache.spark.ml.linalg.Vector


/**
  * 创建FedInstance时要先把key加密了，后面需要用来join
  * @param key
  * @param label
  * @param weight
  * @param features
  * @tparam Encrypted
  */

// Todo 做成LabelFedInstance / CommonFedInstance
case class FedInstance(key:Any,label: Double, weight: Double, features: Vector)
