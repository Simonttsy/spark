package org.apache.spark.ml.fed.encryptor.paillier

trait PrivateKey extends Serializable{

  def decrypt (encrypted: EncryptedNumber):EncodedNumber

}
