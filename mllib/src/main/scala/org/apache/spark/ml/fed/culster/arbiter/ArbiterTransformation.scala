package org.apache.spark.ml.fed.culster.arbiter

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.model.hetero.linr.HeteroLinR
import org.apache.spark.ml.fed.model.homo.HomoLR
import org.apache.spark.ml.fed.model.optimizer.MiniBatchGradientDescent
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class ArbiterTransformation[V](val pipeline: Pipeline, guestIdWithoutArbiter: Array[String]) {

  var optimizer: MiniBatchGradientDescent[V] = _

  var fraction: Double = _

  //Todo 改成 epoch-index : Set[String]  ,防止重启的task ，延迟重发的task等
  //Todo 或者改成epoch-index : Array[String] 每次接收时判断下是否重复，是否老数据等，
  val completedTasks: ArrayBuffer[String] = new ArrayBuffer[String]()

  var seed: Int = _

  // 去中心化，用来聚合的guest
  var aggrSideId: String = _

  var metrics: Array[Double] = _

  def initialize(encryptor: Encryptor): Unit = {
    println("arbiter initialize")

    updateSeed()

    pipeline.getStages.last match {
      case heteroLionR: HeteroLinR[V] =>
        println("heteroLionR initial")
        heteroLionR.setEncryptor(encryptor)
        heteroLionR.initializeLossAggregatorAndoptimizer(null)
        optimizer = heteroLionR.optimizer.asInstanceOf[MiniBatchGradientDescent[V]]
        fraction = heteroLionR.getBatchFraction

        //        heteroLionR.fit()
        println("optimizer ? fraction", optimizer == null, fraction)

      case homoLR: HomoLR[V] =>
        println("homoLR arbiter", homoLR.getClass.getSimpleName)
        println("guestIdWithoutArbiter ->")
        guestIdWithoutArbiter.foreach(print)
        println("=======")

        homoLR.setEncryptor(encryptor)
        homoLR.initializeLossAggregatorAndoptimizer(null)
        optimizer = homoLR.optimizer.asInstanceOf[MiniBatchGradientDescent[V]]
        aggrSideId = homoLR.selectAggrSideRandom(guestIdWithoutArbiter)
        fraction = homoLR.getBatchFraction

        println("aggrSideId is " + aggrSideId)
    }

    println("初始ArbiterTransformation完毕")
  }


  def updateSeed(): Unit = seed = createSeed


  def createSeed: Int = Random.nextInt(99999)


  def execute(dataset: Dataset[_]): Unit = {
    pipeline.fit(dataset: Dataset[_])
  }
}