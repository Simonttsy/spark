package org.apache.spark.ml.fed.model

import org.apache.spark.ml.fed.encryptor.paillier.cli.SerialisationUtil

private[fed] trait ModelMessages extends Serializable


private[fed] object ModelMessages {

  case object SubmitModelTask extends ModelMessages

  //Todo 触发单个pipeline
  /**
    *
    * @param epoch
    * @param blockIndex
    * @param shuffle 第一次fit也是true，初始数据
    */
  case class FitModel(epoch: Long, blockIndex: Long, shuffle: Boolean, seed: Int) extends ModelMessages

  case object Converged extends ModelMessages

  case class CompleteLocalCoefUpdate(guestId: String) extends ModelMessages

  case class MetricsAvg(guestId:String,metrics:Array[Double]) extends ModelMessages

  case class MetricsAvgCompleted(metrics:Array[Double]) extends ModelMessages


  /**
    * HeteroLinR
    */

  case class HeteroLinRFullLossAndGrad(guestId: String,
                                       fullLoss: SerialisationUtil.SerialisedEncrypted,
                                       maskedGrad: Array[SerialisationUtil.SerialisedEncrypted]) extends ModelMessages

  case class HeteroLinRGuestMaskedGrad(guestId: String, maskedGrad: Array[SerialisationUtil.SerialisedEncrypted]) extends ModelMessages

  case class HeteroLinRGuestDecryptedGrad(localMaskedGrad: Array[Double]) extends ModelMessages

  case class HeteroLinRFullResidualArray(key2EncrFullResidualArray: Array[(Any, SerialisationUtil.SerialisedEncrypted)]) extends ModelMessages

  /**
    * HomoLR
    */

  case class HomoLRAvg(guestId: String, serLoss: SerialisationUtil.SerialisedEncrypted, serGrad: Array[SerialisationUtil.SerialisedEncrypted])

  case class HomoLRDecryptStatistics(serLoss: SerialisationUtil.SerialisedEncrypted, serGrad: Array[SerialisationUtil.SerialisedEncrypted])

  //Todo 最好加Mask
  case class HomoLRReportGrad(grad: Array[Double])
}
