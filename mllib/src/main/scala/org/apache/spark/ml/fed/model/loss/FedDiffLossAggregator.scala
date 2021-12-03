package org.apache.spark.ml.fed.model.loss

import org.apache.spark.ml.fed.encryptor.paillier.EncryptedNumber
import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef

/**
  * 专门负责计算fed生产数据集的逻辑，不负责存放数据
  */
//Todo 定义guest，arbiter端分别调用的接口，传参用pojo，每次调用根据pojo类型不同执行不同模型
trait FedDiffLossAggregator extends LossAggregator{


  def initCommonSideLocalVar(instances: RDD[FedInstance],
                             //Todo 增加Array
                             labelSideRefMap: RpcEndpointRef)

  def initLabelSideVar(guestId:String,
                       instances: RDD[FedInstance],
                       batchSize: Double,
                       key2EncryptedWDotXArray: Array[(Any, EncryptedNumber)],
//                       serialisedArray: Array[(Any,SerialisationUtil.SerialisedEncrypted)],
                       otherSideEncrytedLoss: EncryptedNumber,
//                       serialisedOtherSideEncrytedLoss: SerialisationUtil.SerialisedEncrypted,
                       commonSideRefArray: Array[RpcEndpointRef],
                       arbiterRef: RpcEndpointRef)

  def aggregateCommonSideCoef(guestId:String,
                              instances: RDD[FedInstance],
                              batchSize: Double,
                              key2EncrFullResidualArray: Array[(Any, EncryptedNumber)],
                              arbiterRef: RpcEndpointRef)



  def updateCoef(maskedGrad: Array[Double]): Array[Double]

  /**
    * update local loss params for next iteration
    * @param coefficients
    */
  def updateParams(coefficients: Vector)

}
