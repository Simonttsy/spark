package org.apache.spark.ml.fed.model.loss

import breeze.linalg.{DenseVector => BDV}

class DefaultLossFunction() extends DiffFunction[BDV[Double]]{


  override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
null
  }
}
