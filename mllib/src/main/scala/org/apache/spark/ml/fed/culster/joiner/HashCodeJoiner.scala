package org.apache.spark.ml.fed.culster.joiner

import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.internal.Logging
import org.apache.spark.ml.fed.culster.masker.DefaultMasker
import org.apache.spark.ml.fed.utils.Preconditions

import scala.reflect.ClassTag

//Todo 解耦hash和randomDouble ，增加多T类型的Decode

/**
  *
  * @param randomDouble mask
  * @tparam T input type
  * @tparam C coded type
  */
class HashCodeJoiner(randomDouble: Double) extends DefaultMasker(randomDouble: Double) with Joiner with Logging {

    override def innerJoin(thisSeq: Array[Any], otherSeq: Array[Any]): Array[Any] = {

      logInfo(s"this values size :${thisSeq.length},other values size:${otherSeq.length}")

      val joinedKes = (for (thisValue <- thisSeq) yield {

        var flag = (thisValue, false)

        otherSeq.foreach(otherValue => if (thisValue == otherValue) flag = (thisValue, true))
        flag
      }).filter(_._2).map(_._1)

      logInfo("joined values size :" + joinedKes.length)

      joinedKes
    }

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

  object HashCodeJoiner {

    class Builder {
      private[this] var randomDouble: Double = 0.0

      def setRandomRange(origin: Double, bound: Double): Builder = {
        randomDouble = ThreadLocalRandom.current().nextDouble(origin, bound)
        this
      }

      def build(): HashCodeJoiner = {
        Preconditions.checkBooleanCondition(randomDouble == 0, "Random Value is " + randomDouble)

        new HashCodeJoiner(randomDouble)
      }
    }

    def builder[T:ClassTag,C:ClassTag](): Builder = new Builder
  }