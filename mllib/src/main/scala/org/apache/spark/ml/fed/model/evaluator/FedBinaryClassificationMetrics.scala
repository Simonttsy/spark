package org.apache.spark.ml.fed.model.evaluator

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD

class FedBinaryClassificationMetrics(scoreAndLabels: RDD[_ <: Product])
  extends BinaryClassificationMetrics(scoreAndLabels) {

  /**
    *
    * @return threshold,ks
    */
  def kolmogorovSmirnovTest(): (Array[KSTestSubMetrics], KSTestSubMetrics) = {

    val metrics = kolmogorovSmirnovTestEmpiricalCumulativeOrderedDistribution()
    metrics.foreach(println("metrics", _))
    val optimalMetric = metrics.maxBy(_.ks)
    println("最优 threshold, tpr,fpr,ks", KSTestSubMetrics)
    (metrics, optimalMetric)
  }

  /**
    *
    * @return [threshold, tpr, fpr,ks]
    */

  def kolmogorovSmirnovTestEmpiricalCumulativeOrderedDistribution(): Array[KSTestSubMetrics] = {

    val scoreLabelsWeightArray = scoreLabelsWeight.collect()

    println("ks scoreLabelsWeight count ", scoreLabelsWeight.count())

    //Todo 为0 给极小数
    val (trueCount, falseCount) = scoreLabelsWeightArray.aggregate((0D, 0D))((count, scoreLabelsWeight) => {

      val label = scoreLabelsWeight._2._1

      var trueCount = count._1
      var falseCount = count._2

      if (label == 1) {
        trueCount += 1
      } else if (label == 0) {
        falseCount += 1
      } else throw new IllegalArgumentException("label !=0or1,current is " + label)

      (trueCount, falseCount)
    },
      (thisParCount, otherParCount) => {

        (thisParCount._1 + otherParCount._1, thisParCount._2 + otherParCount._2)

      })

    println("trueCount", trueCount, "falseCount", falseCount)

    // 按序迭代，计算结果是按threshold升序，所以后面不需要手动排序
    //    (for (iter <- 0 to 100) yield {
    //      val threshold = iter * 0.01
    //
    //      (threshold, scoreLabelsWeightArray.aggregate((0D, 0D))((count, scoreLabelsWeight) => {
    //
    //        val score = scoreLabelsWeight._1
    //        val label = scoreLabelsWeight._2._1
    //        //Todo 处理weight
    //        val weight = scoreLabelsWeight._2._2
    //
    //        var truePositive = count._1
    //        var falsePositive = count._2
    //
    //        val prediction = if (score >= threshold) 1 else 0
    //
    //        if (prediction == 1) {
    //
    //          if (label == 1) {
    //            truePositive += 1
    //          } else {
    //            falsePositive += 1
    //          }
    //        }
    //
    //        println("truePositive, falsePositive,score", truePositive, falsePositive,score)
    //        (truePositive, falsePositive)
    //      },
    //
    //        (thisTAndFPositive, otherTAndFPositive) => {
    //
    //          //          println("this truePositive",thisTAndFPositive._1)
    //          //          println("other truePositive",otherTAndFPositive._1)
    //          //          println("this falsePositive",thisTAndFPositive._2)
    //          //          println("other falsePositive",otherTAndFPositive._2)
    //          (thisTAndFPositive._1 + otherTAndFPositive._1, thisTAndFPositive._2 + otherTAndFPositive._2)
    //
    //        }
    //      ))
    //    }).map {
    //      case (threshold, (truePositive, falsePositive)) =>
    //
    //        val tpr = truePositive / trueCount
    //        val fpr = falsePositive / falseCount
    //
    //        val ks = tpr - fpr
    //        println("threshold", threshold, "truePositive", truePositive, "falsePositive", falsePositive, "tpr", tpr, "fpr", fpr, "ks", ks)
    //
    //        //        (threshold, ks)
    //        KSTestSubMetrics(threshold, tpr, fpr, ks)
    //    }.toArray


    val threadNum = 100

    // 按序迭代，计算结果是按threshold升序，所以后面不需要手动排序
    // threshold, truePositive,falsePositive
    val metricsBuf = new Array[(Double, Double, Double)](threadNum + 1)

    for (iter <- 0 to threadNum) {
      val threshold = iter * 0.01

      var truePositive = 0
      var falsePositive = 0

      scoreLabelsWeightArray.foreach(element => {

        val score = element._1
        val label = element._2._1
        //Todo 处理weight
        val weight = element._2._2

        val prediction = if (score >= threshold) 1 else 0

        if (prediction == 1) {

          if (label == 1) {
            truePositive += 1
          } else {
            falsePositive += 1
          }
        }
      })

      metricsBuf(iter) = (threshold, truePositive, falsePositive)
    }

    metricsBuf.map {

      case (threshold, truePositive, falsePositive) =>

        val tpr = truePositive / trueCount
        val fpr = falsePositive / falseCount

        val ks = tpr - fpr
        println("threshold", threshold, "truePositive", truePositive, "falsePositive", falsePositive, "tpr", tpr, "fpr", fpr, "ks", ks)

        //        (threshold, ks)
        KSTestSubMetrics(threshold, tpr, fpr, ks)

    }
  }

  /**
    * count true false 总数
    *
    * @return
    */
  def countTF(): (Double, Double) = {

    val zero = (0D, 0D)

    val seqOp: ((Double, Double), (Double, (Double, Double))) => (Double, Double) = (count, scoreLabelsWeight) => {

      val label = scoreLabelsWeight._2._1

      var trueCount = count._1
      var falseCount = count._2

      if (label == 1) {
        trueCount += 1
      } else if (label == 0) {
        falseCount += 1
      } else throw new IllegalArgumentException("label !=0or1,current is " + label)

      (trueCount, falseCount)
    }

    val combOp: ((Double, Double), (Double, Double)) => (Double, Double) = (thisParCount, otherParCount) => {

      (thisParCount._1 + otherParCount._1, thisParCount._2 + otherParCount._2)

    }

    scoreLabelsWeight.aggregate(zero)(seqOp, combOp)
  }

  case class KSTestSubMetrics(threshold: Double, tpr: Double, fpr: Double, ks: Double)

}
