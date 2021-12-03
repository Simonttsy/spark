package org.apache.spark.ml.fed.model.ploter

import breeze.linalg.{DenseVector => BDV}
import breeze.plot._

object Ploter {

  def KSTest(tprs: Array[Double], fprs: Array[Double], thresholds: Array[Double], savePath: String): Unit = {

    val tprVec = new BDV[Double](tprs)
    val fprVec = new BDV[Double](fprs)
    val thresholdVec = new BDV[Double](thresholds)

    val f = Figure()
    val p = f.subplot(0)

    p += plot(thresholdVec, tprVec)
    p += plot(thresholdVec, fprVec, '.')



    val toTprs = tprVec.toArray
    val toFprs = fprVec.toArray
    val toThresholds = thresholdVec.toArray
    for (i<- tprs.indices){

      println("plot ks:Threshold,tpr,fpr->",toThresholds(i),toTprs(i),toFprs(i))

    }

    p.xlabel = "threshold"
    p.ylabel = "tp/fp ratio"

    //"E:\\tmp\\plot\\lines.png"
    f.saveas(savePath)
  }
}
