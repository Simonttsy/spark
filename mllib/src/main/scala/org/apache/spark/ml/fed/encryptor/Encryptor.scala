package org.apache.spark.ml.fed.encryptor

import org.apache.spark.ml.fed.encryptor.paillier.{EncryptedNumber, PaillierPublicKey, PrivateKey}

//Todo 解决非本地计算需要手动序列化问题,但是broadcast后的EncryptedNumber可以相加
trait Encryptor extends Serializable {

  def getPublicKey: PaillierPublicKey

  def encrypt(value: Double):EncryptedNumber

  def decryptToDouble(number: EncryptedNumber,
                      privateKey: PrivateKey): Double = privateKey.decrypt(number).decodeDouble()

}
