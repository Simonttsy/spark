package org.apache.spark.ml.fed.examples

import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.encryptor.paillier.{PaillierPrivateKey, PrivateKey}

object Encrypt {

  def main(args: Array[String]): Unit = {
    val d = ThreadLocalRandom.current().nextDouble(0.001, 123)
    val f = ThreadLocalRandom.current().nextDouble(0.001, 123)

    println(1111111111111111L, d, f)


    val rawNumbers = Array(0.0, 0.8, 1.0, 3.2, -5, 50)

    //    println(rawNumbers)
    val keypair = PaillierPrivateKey.create(1024)
    var publicKey = keypair.getPublicKey
    val context = publicKey.createUnsignedContext()

    val paillierContext = publicKey.createSignedContext

    println("Encrypting doubles with public key (e.g., on multiple devices)")
    val encryptedNumbers = rawNumbers.map(n => paillierContext.encrypt(n))

    println("Adding encrypted doubles")
    val encryptedSum = encryptedNumbers.reduce((n1, n2) => n1.add(n2))

    println("Decrypting result:")
    println(333333, keypair.decrypt(encryptedSum).decodeDouble)


    val number1 = paillierContext.encrypt(d)
    val number2 = paillierContext.encrypt(f)

    //Todo 验证
    number1.equals()

    val ff = number1.add(number2)
    val ffX = ff.multiply(32.414)

    val dff = keypair.decrypt(ff).decodeDouble

    println("B2 = ", d + f,dff )
    println(dff*32.414,keypair.decrypt(ffX).decodeDouble)

    A(keypair,paillierContext)

    val seq = Seq(1,2,3,4,5)

//    ff.mu

  }

  case class A(privateKey:PrivateKey,
               encryptor:Encryptor)
}
