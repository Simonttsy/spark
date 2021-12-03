package org.apache.spark.ml.fed.model.hetero.linr

import org.apache.spark.ml.fed.encryptor.paillier.cli.SerialisationUtil
import org.apache.spark.ml.fed.model.ModelMessages

private[fed] object HeteroLinRMessages extends ModelMessages{


  case class HeteroLinRInitLabelSideVar(key2EncryptedWDotXArray: Array[(Any,SerialisationUtil.SerialisedEncrypted)],encryptedLoss: SerialisationUtil.SerialisedEncrypted)

}
