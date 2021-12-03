package org.apache.spark.ml.fed.model.optimizer

import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.fed.model.optimizer.Gradient.ConvergenceCheck
import org.apache.spark.rdd.RDD

class HomoMiniBatchGradientDescent[V](convergenceCheck: ConvergenceCheck[V])
  extends MiniBatchGradientDescent[V](convergenceCheck) {

  def this(
            //            keys: Array[Any],
            fraction: Double,
            maxIter: Int
          ) = {

    this(Gradient.defaultConvergenceCheck[V](maxIter))
    createBatchWeights(fraction)
  }


  //Todo 做到arbiter端 统一初始化
  var batchWeights: Array[Double] = _

  override def replaceMeta(allInstances: RDD[FedInstance],replaceSeed: Int): Array[RDD[FedInstance]] = {
    println("randomSplit")
    println("batchWeights ->")
    println("batchWeights.length ",batchWeights.length)
    allInstances.randomSplit(batchWeights,seed = replaceSeed)
  }

  /**
    * 只能取大概，RDD splitRandom 加合！=1  时会自动标准化
    * 适用于Homo
    *
    * @param fraction
    * @return
    */   //Todo 做到arbiter端 统一初始化
  def createBatchWeights(fraction: Double): Unit = {

    val formated = fraction.formatted("%.2f").toDouble

    println("formated ", formated)

    val splitNum = math.floor(1 / formated).toInt

    blockNumber = splitNum
    println("splitNum", splitNum)

    batchWeights = Array.fill(splitNum)(formated)
    println("homoBatchWeights ->")

    batchWeights.foreach(print)
  }

}
