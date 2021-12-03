package org.apache.spark.ml.fed.model.loss

import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.encryptor.paillier.EncryptedNumber
import org.apache.spark.ml.fed.encryptor.paillier.cli.SerialisationUtil
import org.apache.spark.ml.fed.model.ModelMessages.{HomoLRAvg, HomoLRDecryptStatistics}
import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef

import scala.collection.mutable.ArrayBuffer

//Todo 部分方法做成公共方法类里
/** A 发送公钥给所有c
  * 挑选一个c_aggr作为统计总loss
  * 所有端的Grad 加密后发送给c_aggr 求loss
  * c_aggr 发送loss给 A
  * A解密后发送给所有c
  *
  * 这里的A其实可以以后优化成 挑选一个非c_aggr作为这个A,去中心化
  *
  * @param coefficients
  * @param fitIntercept
  * @param regParam
  * @param lambda scale penalty
  */
//Todo 做个跌点检查机制 接口 每次迭代 检查coef 等是否更上次相同
class LRLossFunction(selfGuestId: String,
                     var coefficients: Vector, //updateParams方法更新
                     fitIntercept: Boolean,
                     regParam: Double,
                     lambda: Double = 0,
                     stepSize: Double, // 只在updateCoefficients处理
                     encryptor: Encryptor) extends LossAggregator with Serializable {

  //on aggr side ，统计每阶段完成的guest

  //Todo 这种模式 guest<=2 会有数据泄露问题，>2没有

  //Todo 改成 epoch-index : Set[String]  ,防止重启的task ，延迟重发的task等
  //Todo 或者改成epoch-index : Array[String] 每次接收时判断下是否重复，是否老数据等，
  val receivedTask: ArrayBuffer[TaskDescription] = new ArrayBuffer[TaskDescription]()

  //Todo  intercept 取值范围~
  val intercept: Double = if (fitIntercept) 0.0025 else 0

  def reportLossAndGrad(loss: Double, grad: Array[Double], ref: RpcEndpointRef): Unit = {
    val serLoss = SerialisationUtil.serialiseEncrypted(encryptor.encrypt(loss))
    val serGrad = grad.map(pd => SerialisationUtil.serialiseEncrypted(encryptor.encrypt(pd)))

    println("send(HomoLRAvg")
    ref.send(HomoLRAvg(selfGuestId, serLoss, serGrad))
  }


  def decryptAvgStatistics(loss: EncryptedNumber, grad: Array[EncryptedNumber], arbiter: RpcEndpointRef): Unit = {
    val serLoss = SerialisationUtil.serialiseEncrypted(loss)
    val serGrad = grad.map(SerialisationUtil.serialiseEncrypted)

    println("send(HomoLRDecryptStatistics")
    arbiter.send(HomoLRDecryptStatistics(serLoss, serGrad))
  }

  def updateCoefficients(grad: Array[Double]): Vector = {

    println("updateCoefficients , stepSize", stepSize)
    val updatedCoefVector = Vectors.dense(coefficients.toArray.zip(grad).map {
      case (weight, partialDerivative) =>
        println("weight", weight, "partialDerivative", partialDerivative)
        weight - (stepSize * partialDerivative)
    })

    println("updatedCoef -> ", updatedCoefVector.toArray.mkString(","))
    updateParams(updatedCoefVector)

    updatedCoefVector
  }

  /**
    * 更新系数等参数
    *
    * @param coefficients
    */
  def updateParams(coefficients: Vector): Unit = this.coefficients = coefficients

  def computeBinaryLossAndGrad(instances: RDD[FedInstance], batchSize: Double): (Double, Array[Double]) = {
    println("computeBinaryLossAndGrad")
    println("====================")
    println("coefficients", coefficients.toArray.mkString(","))

    val bcCoef = instances.context.broadcast(coefficients)
    //    val bcBatchSize = instances.context.broadcast(batchSize)
    val bcIntercept = instances.context.broadcast(intercept)
    //    val bcRegParam = instances.context.broadcast(regParam)
    //    val bcLambda = instances.context.broadcast(lambda)

    val (sumGradNorm, sumLossNorm) = instances.mapPartitions(iter => {

      val coef = bcCoef.value
      //      val size = bcBatchSize.value
      val interceptValue = bcIntercept.value
      //      val penaltyType = bcRegParam.value
      //      val scalePenalty = bcLambda.value

      iter.map(instance => {
        val y = instance.label
        val xVector = instance.features

        //Todo (y,NaN)

        val score = computeScore(xVector, coef, interceptValue)
        val residual = score - y
        //        val tmpLambda = scalePenalty / size

        println("score", score, "residual", residual,
          "x", xVector.toArray.mkString(","), "y", y)

        // (h(xi) - yi)*xij
        val gradNorm = xVector.toArray.map(_ * residual)

        //        // (h(xi) - yi)*xij + λ/m*θj
        //        val grad = xVector.toArray.zip(coef.toArray).map {
        //          case (x, w) =>
        ////            println("residual * x + tmpLambda * w = " + residual * x + tmpLambda * w)
        //            residual * x + tmpLambda * w
        //        }

        //Todo 提前检测Label分布，是否满足1,0
        //label = 1 其他视为0
        val lossNorm = if (y == 1) {
          -math.log(score)
        } else {
          -math.log(1 - score)
        }

        println("single lossNorm", lossNorm)
        println("single gradNorm", gradNorm.mkString(","))
        (gradNorm, lossNorm)
      })
    }).reduce((tup1, tup2) => {
      val thisGrad = tup1._1
      val thisLoss = tup1._2

      val otherGrad = tup2._1
      val otherLoss = tup2._2

      println("thisGrad,thisLoss", thisGrad.mkString(","), thisLoss,
        "otherGrad,otherLoss", otherGrad.mkString(","), otherLoss)

      val sumGrad = thisGrad.zip(otherGrad).map {
        // partial derivative
        case (thisPar, otherPar) =>
          //          println("thisPar,otherPar", thisPar, otherPar)
          thisPar + otherPar
      }

      val sumLoss = thisLoss + otherLoss

      (sumGrad, sumLoss)
    })

    println("batchSize", batchSize)

    var loss = sumLossNorm / batchSize

    println("sumLossNorm", sumLossNorm, "lossNorm", loss)

    var grad = sumGradNorm.map(_ / batchSize)

    println("sumGradNorm", sumGradNorm.mkString(","))
    println("gradNorm", grad.mkString(","))

    //    //Todo 生成注掉
    //    println("instances.count", instances.count())

    if (regParam == 0) {
      loss = loss + (computeL2Penalty(coefficients, lambda) / (2 * batchSize))

      grad = grad.zip(coefficients.toArray).map {
        case (pd, w) =>
          // pd(partial derivative)->(h(xi) - yi)*xij
          //pd + λ/m*θj
          pd + (lambda * w / batchSize)
      }
    }
    println("loss", loss)
    println("grad", grad.mkString(","))

    bcCoef.destroy()
    //    bcBatchSize.destroy()
    bcIntercept.destroy()
    //    bcRegParam.destroy()
    //    bcLambda.destroy()

    (loss, grad)
  }


  def computeBinaryLoss(instances: RDD[FedInstance], batchSize: Int): Double = {
    val bcCoef = instances.context.broadcast(coefficients)
    val bcIntercept = instances.context.broadcast(intercept)
    val bcLambda = instances.context.broadcast(lambda)

    val sumLoss = instances.mapPartitions(iter => {

      val coef = bcCoef.value
      val interceptValue = bcIntercept.value
      val scalePenalty = bcLambda.value

      iter.map(instance => {
        val label = instance.label
        val x = instance.features

        val score = computeScore(x, coef, interceptValue)


        //Todo 提前检测Label分布，是否满足1,0
        //这里只需要有label = 1 就行 ，其他视为0
        var lossNorm = if (label == 1) {
          math.log(score)
        } else {
          1 - math.log(score)
        }


        if (regParam == 0) { //Todo 放在loss求和完再加 提升性能？
          lossNorm += computeL2Penalty(coef, scalePenalty)
        }

        lossNorm
      })
    }).sum()


    bcCoef.destroy()
    bcIntercept.destroy()
    bcLambda.destroy()


    0.5 * (1 / batchSize) * sumLoss
  }


  def computeGradient(instances: RDD[FedInstance], batchSize: Int): Array[Double] = {
    val bcCoef = instances.context.broadcast(coefficients)
    val bcBatchSize = instances.context.broadcast(batchSize)
    val bcIntercept = instances.context.broadcast(intercept)
    val bcLambda = instances.context.broadcast(lambda)

    val grad = instances.mapPartitions(iter => {

      val coef = bcCoef.value
      val size = bcBatchSize.value
      val interceptValue = bcIntercept.value
      val scalePenalty = bcLambda.value

      iter.map(instance => {

        val y = instance.label
        val xVector = instance.features

        val score = computeScore(xVector, coef, interceptValue)
        val residual = score - y
        val tmpLambda = scalePenalty / size

        println("score", score, "residual", residual, "tmpLambda", tmpLambda,
          "x", xVector.toArray.mkString(","), "y", y)

        // (h(xi) - yi)*xij + λ/m*θj
        xVector.toArray.zip(coef.toArray).map {
          case (x, w) =>
            //            println("residual * x + tmpLambda * w = " + residual * x + tmpLambda * w)
            residual * x + tmpLambda * w
        }
      })
    })
      // sum grad
      .reduce(

      (thisGrad, otherGrad) => {

        println("thisGrad", thisGrad.mkString(","), "otherGrad", otherGrad.mkString(","))

        thisGrad.zip(otherGrad).map {
          // partial derivative
          case (thisPar, otherPar) =>
            thisPar + otherPar
        }

      }

    ) // mean
      .map(_ / batchSize)

    bcCoef.destroy()
    bcBatchSize.destroy()
    bcIntercept.destroy()
    bcLambda.destroy()

    grad
  }


  // on aggr side
  def averaging(denominator: Double): (EncryptedNumber, Array[EncryptedNumber]) = {

    val meanNum: Double = 1.0 / denominator

    println("receivedTask.size", denominator, "meanNum", meanNum)

    receivedTask.foreach(a => {
      println("received id,gard,loss ->", a.guestId, a.grad.mkString(","), a.loss)
    })

    val sumTask = receivedTask.reduce {
      (t1, t2) =>
        val sumGrad = t1.grad.zip(t2.grad).map {
          case (pd1, pd2) => pd1.add(pd2)
        }
        val sumLoss = t1.loss.add(t2.loss)
        TaskDescription("", sumLoss, sumGrad)
    }

    val sumLoss = sumTask.loss
    val sumGrad = sumTask.grad

    val avgLoss = sumLoss.multiply(meanNum)
    val avgGrad = sumGrad.map(_.multiply(meanNum))

    (avgLoss, avgGrad)
  }

  //  def scoreWithIntercept(x: Vector, w: Vector, fitIntercept: Boolean): Double = {
  //    if (!fitIntercept) {
  //      computeScore(x, w, 0)
  //    } else { //Todo 截距赋值范围
  //      computeScore(x, w, 0.025)
  //    }
  //  }


  def computeScore(x: Vector, w: Vector, intercept: Double): Double = {
    1.0 / (1.0 + math.exp(-(x.dot(w) + intercept)))
  }

  //Todo 增加 L1 ，Elastic
  //Todo * 0.5？？？？ Fate是* 0.5

  def computeL2Penalty(w: Vector, scalePenalty: Double): Double = {
    scalePenalty * w.toArray.map(math.pow(_, 2)).sum
  }
}

case class TaskDescription(guestId: String, loss: EncryptedNumber, grad: Array[EncryptedNumber])