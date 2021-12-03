package org.apache.spark.ml.fed.culster.guest

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.fed.model.hetero.linr.HeteroLinR
import org.apache.spark.ml.fed.model.homo.HomoLR
import org.apache.spark.ml.fed.model.loss.{FedDiffLossAggregator, LRLossFunction}
import org.apache.spark.ml.fed.model.optimizer.MiniBatchGradientDescent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class GuestTransformation[V]() {

  var pipeline: Pipeline = _

  //Todo lossAggregator做成统一接口
  var lossAggregator: FedDiffLossAggregator = _ // linr

  var homoLRLossAggregator: LRLossFunction = _

  var optimizer: MiniBatchGradientDescent[V] = _

  var hasLabel: Boolean = false

  var fraction: Double = _

  //Todo 做成最后需要检测是否持久化有就取消
  var maybePersistedBlockData: (RDD[FedInstance], Boolean) = _

  var mask: Double = _

  //Todo 保证各端数据大小，顺序一致，后面随机seed通过arbiter发送
  var keys: Array[Any] = _

  var userPrimaryKey: String = _

  var encryptor: Encryptor = _

  var modelId: String = _

  var allInstances: RDD[FedInstance] = _

  var trainInstances: RDD[FedInstance] = _

  var testDF: DataFrame = _


  def initialize(allSamples: DataFrame): Unit = {

    //Todo 统一接口
    pipeline.getStages.last match {
      case heteroLionR: HeteroLinR[V] =>

        val bcJoinedKeys = allSamples.sparkSession.sparkContext.broadcast(keys)
        val bcPK = allSamples.sparkSession.sparkContext.broadcast(userPrimaryKey)

        println("userPrimaryKey = " + userPrimaryKey)

        var filteredDataset = allSamples.filter(elem => {
          bcJoinedKeys.value.contains(elem.getAs[Any](bcPK.value))
        })

        println("allSamples.count " + allSamples.count()) //allSamples.count 15

        println("keys.length", keys.length, "filteredDataset.count() = " + filteredDataset.count())

        filteredDataset.show(2)

        pipeline.getStages.foreach {
          case assembler: VectorAssembler =>
            println("execute assembler")
            filteredDataset = assembler.transform(filteredDataset)
          case _ =>
        }

        filteredDataset.show(3)

        //Todo 像这样的初始化做到组开始的接口中
        heteroLionR.setEncryptor(encryptor)
        heteroLionR.setPrimarykeyCol(userPrimaryKey)
        heteroLionR.setJoinedkeys(keys)

        val trainAndtest = filteredDataset.randomSplit(Array(0.7, 0.3))

        trainInstances = heteroLionR.initialize(trainAndtest(0))
        testDF = trainAndtest(1)

        lossAggregator = heteroLionR.lossAggregator
        println("HeteroLinR ", lossAggregator.getClass.getSimpleName)
        hasLabel = heteroLionR.getFedLabel
        optimizer = heteroLionR.optimizer.asInstanceOf[MiniBatchGradientDescent[V]]
        fraction = heteroLionR.getBatchFraction

        mask = heteroLionR.getMask
        modelId = heteroLionR.uid

      case homoLR: HomoLR[V] =>

        homoLR.setEncryptor(encryptor)

        var transformed: DataFrame = null

        //Todo 统一
        pipeline.getStages.foreach {
          case assembler: VectorAssembler =>
            println("execute assembler")
            transformed = assembler.transform(allSamples)
          case _ =>
        }
        println("split:", homoLR.getTrainingSetRatio, 1 - homoLR.getTrainingSetRatio)
        val split = Array(homoLR.getTrainingSetRatio, 1 - homoLR.getTrainingSetRatio)

        val trainAndtest = if (transformed != null) {
          println("initialize transformed with splited train and test")
          transformed.randomSplit(split)
        } else {
          println("initialize allSamples with splited train and test")
          allSamples.randomSplit(split)
        }

        trainInstances = homoLR.initialize(trainAndtest(0))
        testDF = trainAndtest(1)

        homoLRLossAggregator = homoLR.lossAggregator
        optimizer = homoLR.optimizer.asInstanceOf[MiniBatchGradientDescent[V]]
        println("homoLRLossAggregator,", homoLRLossAggregator.getClass.getSimpleName)
        fraction = homoLR.getBatchFraction

        modelId = homoLR.uid

    }

    //不落磁盘，提高性能
    //Todo job 结束后 unpersist
    trainInstances.cache()
    println("instanceRDD.foreach开始")
    println("in put fed instance =>")
    println("in put fed instance =>")
    trainInstances.take(10).foreach(println)
    println("<= in put fed instance")
    println("<= in put fed instance")

    println("in put fed instance count is ", trainInstances.count())

    println("unpersist allSamples")
    allSamples.unpersist()
  }
}
