package org.apache.spark.ml.fed.model

import org.apache.spark.ml.linalg.{Vector, Vectors}
import breeze.linalg.{Vector => BV}

object CommonStatistics {

  def l2Reg(vec: Array[Double], lambda: Double): Double = l2Reg(Vectors.dense(vec), lambda)

  def l2Reg(vec: Vector, lambda: Double): Double = l2Reg(vec.asBreeze, lambda)

  def l2Reg(vec: BV[Double], lambda: Double): Double = (lambda / 2) * vec.map(math.pow(_, 2)).sum

}
