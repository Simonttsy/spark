package org.apache.spark.ml.fed.model.optimizer

import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.fed.model.loss.DiffFunction
import org.apache.spark.ml.fed.model.optimizer.Gradient.ConvergenceCheck
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//Todo 每次batch 是否有放回？
/**
  * 负责minibatch梯度的逻辑，负责batch数据的存储和更新
  *
  * @param batchFraction
  * @param epoch
  * @param convergenceCheck
  * @param classTag$T
  * @tparam T
  */

//Todo +初始化逻辑的接口，不同模型初始化时调用该接口
abstract class MiniBatchGradientDescent[V](
                                            //                                                diff: DiffFunction[RDD[FedInstance[T]], V],
                                            convergenceCheck: ConvergenceCheck[V])
  extends Gradient[V, DiffFunction[V]](convergenceCheck) {

  //current
  @volatile var index: Long = 1
  //current
  @volatile var epoch: Long = 1

  var blockNumber: Int = 0

  val stateBuf: mutable.ArrayBuffer[State] = new ArrayBuffer[State]()

  // 必须浮点类型，计算时 结果也会为浮点 不会自动转整数
  var batchSize: Double = _

  var batchInstance: Array[RDD[FedInstance]] = _

  var curBatch:RDD[FedInstance] =_

  /**
    * batchInstance start from 0
    * @param blockIndex start from 1
    * @return
    */
  def extractBlockData(blockIndex: Long): RDD[FedInstance] = {
    println("extractBlockData")
    println("cur index ="+(blockIndex -1).toInt)
    curBatch = batchInstance((blockIndex -1).toInt)
    curBatch.cache()
    batchSize = curBatch.count().toDouble
    println("batchSize ="+batchSize)
    curBatch
  }

  //Todo remove curBatch from batchInstance
  def unpersistCurBatch(): Unit ={
    curBatch.unpersist()
  }

  /**
    * Any history the derived minimization function needs to do its updates. typically an approximation
    * to the second derivative/hessian matrix.
    */
  override type History = MiniBatchGradientDescent.History

  //Todo 改
  //  override def initialHistory(f: DiffFunction[RDD[FedInstance[T]]], init: RDD[FedInstance[T]]): MiniBatchGradientDescent.History = {
  //    MiniBatchGradientDescent.History(0.0, null, 0)
  //  }
  //
  //  //Todo 改
  //  override def updateHistory(newX: RDD[FedInstance[T]], newGrad: RDD[FedInstance[T]], newVal: Double, f: DiffFunction[RDD[FedInstance[T]]], oldState: State): MiniBatchGradientDescent.History = {
  //    new MiniBatchGradientDescent.History(0.0, null, 11)
  //  }

  def shuffleData(allInstances: RDD[FedInstance],seed: Int): Unit ={
    println("shuffleData...seed",seed)
    batchInstance = replaceMeta(allInstances,seed)
  }

  protected def replaceMeta(allInstances: RDD[FedInstance],seed: Int):Array[RDD[FedInstance]]


}

object MiniBatchGradientDescent {

  case class History(loss: Double, grad: Array[Double], epoch: Long)

}


case class FedState(epoch: Long, loss: Double,
                    coefficients: Vector)