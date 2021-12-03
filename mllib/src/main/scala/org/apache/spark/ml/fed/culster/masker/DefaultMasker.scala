package org.apache.spark.ml.fed.culster.masker

import org.apache.spark.ml.fed.encryptor.Masker
import org.apache.spark.ml.fed.utils.HashParms

class DefaultMasker(randomDouble: Double) extends HashParms with Masker[Double]{

  private def hashSeq(seq: Array[Any]): Array[Int] = seq.map($).filter(_.nonEmpty).map(_.get)

  override def code(seq: Array[Any]): Array[Double] = seq.map(code).filter(_.nonEmpty).map(_.get)

  override def decode(seq: Array[Double]): Array[Any] = seq.map(_ - randomDouble)

  override def code(i: Any): Option[Double] = {
    $(i) match {
      case Some(value) => Some(value + randomDouble)
      case None => None
    }
  }

  override def toCodedMap(v: Array[Any]): Map[Double, Any] = {
    v.map(v => {

      val codeValueOpt = code(v)

      (v, codeValueOpt)
    })
      .filter(_._2.nonEmpty).map(t => (t._2.get, t._1)).toMap


  }
}
