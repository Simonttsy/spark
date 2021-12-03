package org.apache.spark.ml.fed.model.loss

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.encryptor.paillier.EncryptedNumber
import org.apache.spark.ml.fed.encryptor.paillier.cli.SerialisationUtil
import org.apache.spark.ml.fed.model.CommonStatistics
import org.apache.spark.ml.fed.model.ModelMessages._
import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.fed.model.hetero.linr.HeteroLinRMessages.HeteroLinRInitLabelSideVar
import org.apache.spark.ml.fed.utils.Preconditions
import org.apache.spark.ml.linalg.BLAS._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.StorageLevel

// func决定支持集群和伪数据集
class FedLinRLossFunction(lossAggregator: FedDiffLossAggregator,
                          labelSideRefMap: RpcEndpointRef,
                          hasLabel: Boolean) extends DiffFunction[Vector] {
  /**
    * Calculates both the value and the gradient at a point
    *
    * @param x ????
    * @return
    */
  override def calculate(w: Vector): (Double, Vector) = (0, null)
}


//Todo Loss未求均值，梯度求了均值，会有影响吗？？？？
class FedLinRDiffLossAggregator(var coefficients: Vector,
                                fitIntercept: Boolean,
                                regParam: Double,
                                lambda: Double = 0, //不开启正则时=0
                                stepSize: Double,
                                mask: Double, // Utils.random(0.001, 100.0)
                                encryptor: Encryptor) extends FedDiffLossAggregator {

  //regParam=0 -> l2 ->lambda!=0
  Preconditions.checkBooleanCondition(regParam == 0 && lambda == 0, s"regParam = $regParam but lambda = $lambda")

  Preconditions.checkBooleanCondition(regParam != 0 && regParam != 3, s"Support only L2 regularization or NONE(regParam = 3),regParam is " + regParam)

  /**
    * step 1-2
    *
    * @param instances
    * @tparam T
    */
  def initCommonSideLocalVar(instances: RDD[FedInstance],
                             labelSideRefMap: RpcEndpointRef): Unit = {

    val bcCoefficients = instances.context.broadcast(coefficients)

    //Todo 后面做成executor之间发送
    var tuple2Array = instances.map(instance => {

      val coefVector = bcCoefficients.value

      //ua = ub <=> ui = θ·Xi
      val wx = dot(instance.features, coefVector)

      println("instance.features.toString = " + instance.features.toString,
        "bcCoefficients.value = " + coefVector,
        "wx =" + wx)

      (instance.key, wx, math.pow(wx, 2))
    }).collect()

    val key2EncryptedWDotXArray = tuple2Array.map(t => (t._1, SerialisationUtil.serialiseEncrypted(encryptor.encrypt(t._2))))

    //loss_norm + regL2
    val encrytedLoss = encryptor.encrypt(tuple2Array.map(_._2).sum + CommonStatistics.l2Reg(bcCoefficients.value, lambda))

    //Todo 序列化+压缩，后面做成executor之间发送
    println("send(HeteroLinRInitLabelSideVar")
    println("encrytedLoss", encrytedLoss)

    val serialiseEncrypted = SerialisationUtil.serialiseEncrypted(encrytedLoss)

    labelSideRefMap.send(HeteroLinRInitLabelSideVar(key2EncryptedWDotXArray, serialiseEncrypted))

    bcCoefficients.destroy()
  }


  /**
    * 初始化标签方
    *
    * @param instances
    * @param guestCoefVector
    * @param key2EncryptedWDotXArray
    * @param encrytedLoss
    * @tparam T Encrypted Key Type
    */
  def initLabelSideVar(guestId: String,
                       instances: RDD[FedInstance],
                       batchSize: Double,
                       key2EncryptedWDotXArray: Array[(Any, EncryptedNumber)],
                       //                       serialisedArray: Array[(Any,SerialisationUtil.SerialisedEncrypted)],
                       otherSideEncrytedLoss: EncryptedNumber,
                       //                       serialisedOtherSideEncrytedLoss: SerialisationUtil.SerialisedEncrypted,
                       commonSideRefArray: Array[RpcEndpointRef],
                       arbiterRef: RpcEndpointRef): Unit = {

    println("initLabelSideVar")

    println("encryptor ==null" + encryptor == null)
    println("guestId", guestId, "instances.toString", instances.toString, "batchSize", batchSize,
      "key2EncryptedWDotXArray.size", key2EncryptedWDotXArray.length, "otherSideEncrytedLoss", otherSideEncrytedLoss,
      "commonSideRefArray.size", commonSideRefArray.length, "localEncryptor==null", arbiterRef == null)

    //Todo 解决非本地计算需要手动序列化问题
    //Todo 增加截距
    //Todo 支持多side

    //只有kv2个值，就算条数多数据量也不多
    val bcKey2WXArray = instances.context.broadcast(key2EncryptedWDotXArray)

    //    val bcKey2WXArray = instances.context.broadcast(serialisedArray)
    val bcThisCoefVector = instances.context.broadcast(coefficients)
    val bcEncryptor = instances.context.broadcast(encryptor)


    val tuple5Vars = instances.mapPartitions(iter => {

      val enc = bcEncryptor.value
      val key2WXArray = bcKey2WXArray.value
      val thisCoefVector = bcThisCoefVector.value

      key2WXArray.foreach(a => {
        println("k", a._1, a._2)
      })

      for (instance <- iter) yield {

        println("======??======")

        val wx = dot(instance.features, thisCoefVector)
        println("wx = " + wx)

        println("instance.key = " + instance.key)

        //just one term joined
        //Todo 用== ？hash过的
        //匹配不到值 下面 add会卡住
        val (_, otherPartyEncryptedWX) = key2WXArray.filter(_._1 == instance.key).head

        println("otherPartyEncryptedWX", otherPartyEncryptedWX)

        val thisResidual = wx - instance.label

        println("thisResidual", thisResidual)

        //[[di]] = [[uAi]] + [[uBi − yi]]
        val encrFullResidual = otherPartyEncryptedWX.add(enc.encrypt(thisResidual))

        println("encrFullResidual", encrFullResidual)
        //[[uAi]](uBi − yi)
        val lossIntermediate = otherPartyEncryptedWX.multiply(thisResidual)

        println("lossIntermediate", lossIntermediate)

        //[[di]]xBi
        //        val gradNorm = instance.features.toArray.map(encrFullResidual.multiply)
        val serGradNorm = instance.features.toArray.map(x => SerialisationUtil.serialiseEncrypted(encrFullResidual.multiply(x)))

        println("serGradNorm", serGradNorm)

        val serFullResidual = SerialisationUtil.serialiseEncrypted(encrFullResidual)
        val serLossIntermediate = SerialisationUtil.serialiseEncrypted(lossIntermediate)


        //        (instance.key, encrFullResidual, math.pow(thisResidual, 2), lossIntermediate, gradNorm)
        (instance.key, serFullResidual, math.pow(thisResidual, 2), serLossIntermediate, serGradNorm)

      }

    })
      .persist(StorageLevel.MEMORY_AND_DISK)

    // send to other party
    // Todo 做成executor端send

    val key2EncrFullResidualArray = tuple5Vars.map(t => (t._1, t._2)).collect()


    var (thisLossNorm, serLossIntermediate, serGradNorm) = tuple5Vars
      .map(t => (t._3, t._4, t._5))
      .reduce {
        case (stats1, stats2) =>

          val key = bcEncryptor.value.getPublicKey

          println("stats1, stats2", stats1, stats2)

          //Σi((uBi − yi)2)
          val sigmaThisResidual = stats1._1 + stats2._1

          println("sigmaThisResidual", sigmaThisResidual)

          //Σ[[uAi]](uBi − yi)

          val thisLossIntermediate = SerialisationUtil.unserialiseEncrypted(stats1._2, key)
          val otherLossIntermediate = SerialisationUtil.unserialiseEncrypted(stats2._2, key)

          val serSigmaLossIntermediate = SerialisationUtil.serialiseEncrypted(thisLossIntermediate.add(otherLossIntermediate))

          println("serSigmaLossIntermediate", serSigmaLossIntermediate)
          val gradNorm1 = stats1._3
          val gradNorm2 = stats2._3

          println("gradNorm1,gradNorm2", gradNorm1, gradNorm2)
          //Σ[[di]]xAi
          val serGradNorm = gradNorm1.zip(gradNorm2).map(zipped => {

            val thisGradNorm = SerialisationUtil.unserialiseEncrypted(zipped._1, key)
            val otherGradNorm = SerialisationUtil.unserialiseEncrypted(zipped._2, key)
            SerialisationUtil.serialiseEncrypted(thisGradNorm.add(otherGradNorm))

          })

          println("gradNorm=========" + serGradNorm)
          (sigmaThisResidual, serSigmaLossIntermediate, serGradNorm)
      }


    println("thisLossNorm", thisLossNorm)
    println("serLossIntermediate", serLossIntermediate)
    println("serGradNorm", serGradNorm)

    val (lossIntermediate, gradNorm) = (SerialisationUtil.unserialiseEncrypted(serLossIntermediate, encryptor.getPublicKey),
      serGradNorm.map(SerialisationUtil.unserialiseEncrypted(_, encryptor.getPublicKey)))

    if (regParam == 0) {

      println("L2:" + regParam)
      thisLossNorm = thisLossNorm + CommonStatistics.l2Reg(coefficients, lambda)
    } else {
      println("none regParam :" + regParam)
    }

    val fullLoss = otherSideEncrytedLoss.add(encryptor.encrypt(thisLossNorm).add(lossIntermediate.multiply(2)))
    println("fullLoss", fullLoss)

    //Todo 确认是否是单特征+对应θ

    val grad = comGradientWithReg(gradNorm, batchSize, coefficients.toArray, regParam, lambda)

    println("mask", mask)
    grad.foreach(a => {
      println(11, a)
    })
    println("grad", grad)
    val serMaskedGrad = grad.map(pd => SerialisationUtil.serialiseEncrypted(pd.add(encryptor.encrypt(mask))))

    println("serMaskedGrad", serMaskedGrad)


    val serFullLoss = SerialisationUtil.serialiseEncrypted(fullLoss)

    println("send(HeteroLinRFullLossAndGrad")

    arbiterRef.send(HeteroLinRFullLossAndGrad(guestId, serFullLoss, serMaskedGrad))

    println("send(HeteroLinRFullResidualArray")
    commonSideRefArray.foreach(_.send(HeteroLinRFullResidualArray(key2EncrFullResidualArray)))

    tuple5Vars.unpersist()
    bcKey2WXArray.destroy()
    bcThisCoefVector.destroy()
    bcEncryptor.destroy()

  }

  def aggregateCommonSideCoef(guestId: String,
                              instances: RDD[FedInstance],
                              batchSize: Double,
                              key2EncrFullResidualArray: Array[(Any, EncryptedNumber)],
                              arbiterRef: RpcEndpointRef): Unit = {

    val bcVars = instances.context.broadcast(key2EncrFullResidualArray)
    val bcEncryptor = instances.context.broadcast(encryptor)

    val serGradNorm = instances.mapPartitions(iter => {
      val vars = bcVars.value

      for (inst <- iter) yield {
        // just one term
        val (_, fullResidual) = vars.filter(_._1 == inst.key).head

        println("aggregateCommonSideCoef - fullResidual", fullResidual)
        //[[di]]xBi
        //        val tmp = inst.features.toArray.map(fullResidual.multiply)

        val tmp = inst.features.toArray.map(x => SerialisationUtil.serialiseEncrypted(fullResidual.multiply(x)))
        println("aggregateCommonSideCoef - tmp", tmp.toString)

        tmp
      }
    })
      .reduce((gradNorm1, gradNorm2) => {
        //Σ[[di]]xAi
        gradNorm1.zip(gradNorm2).map(zipped => {
          val key = bcEncryptor.value.getPublicKey

          val var1 = SerialisationUtil.unserialiseEncrypted(zipped._1, key)
          val var2 = SerialisationUtil.unserialiseEncrypted(zipped._2, key)

          SerialisationUtil.serialiseEncrypted(var1.add(var2))

        })
      })

    val gradNorm = serGradNorm.map(SerialisationUtil.unserialiseEncrypted(_, encryptor.getPublicKey))


    println("aggregateCommonSideCoef - gradNorm = " + gradNorm)
    val grad = comGradientWithReg(gradNorm, batchSize, coefficients.toArray, regParam, lambda)

    println("aggregateCommonSideCoef - grad = " + grad)

    val serMaskedGrad = grad.map(pd => SerialisationUtil.serialiseEncrypted(pd.add(encryptor.encrypt(mask))))
    println("aggregateCommonSideCoef - serMaskedGrad = " + serMaskedGrad)

    println("HeteroLinRGuestMaskedGrad")
    arbiterRef.send(HeteroLinRGuestMaskedGrad(guestId, serMaskedGrad))

    bcVars.destroy()
    bcEncryptor.destroy()
  }


  def updateCoef(maskedGrad: Array[Double]): Array[Double] = {

    //Todo logo 折线图
    val grad = maskedGrad.map(_ - mask)

    println("stepSize " + stepSize)

    coefficients.toArray.zip(grad).map {
      case (weight, partialDerivative) =>
        weight - stepSize * partialDerivative
    }

  }


  /**参考fate源码
    * if self.penalty == consts.L2_PENALTY:
    *   if lr_weights.fit_intercept:
    *     gradient_without_intercept = grad[: -1]
    *     gradient_without_intercept += self.alpha * lr_weights.coef_
    *     new_grad = np.append(gradient_without_intercept, grad[-1])
    *   else:
    *     new_grad = grad + self.alpha * lr_weights.coef_
    * else:
    * new_grad = grad
    *
    * @param gradNorm
    * @param examplesSize
    * @param coefficients
    * @param regParam
    * @param lambda
    * @return
    */
  private def comGradientWithReg(gradNorm: Array[EncryptedNumber],
                                 examplesSize: Double,
                                 coefficients: Array[Double],
                                 regParam: Double,
                                 lambda: Double): Array[EncryptedNumber] = {

    println("examplesSize", examplesSize)
    println("coefficients", coefficients)
    println("regParam", regParam)
    println("lambda", lambda)
    println("gradNorm", gradNorm)

    //Todo 求均值是否影响模型？？论文上是不带求均值的！Fate源码是有求均值

    val mean: Double = 1 / examplesSize

    //mean gradient
    var grad = gradNorm.map(par => par.multiply(mean))

    println("grad111 =" + grad)

    if (regParam == 0) {

      // + [[λΘA]]
      grad = grad.zip(coefficients).map {
        case (partialDerivative, w) =>
          println("partialDerivative, w", partialDerivative, w)
          //Todo 是否 mean * w，参考了fate源码 应该是下面那种
          //partialDerivative.add(encryptor.encrypt(lambda * mean * w))
          partialDerivative.add(encryptor.encrypt(lambda * w))
      }
    }
    println("grad222 =" + grad)

    grad
  }

  override def updateParams(coefficients: Vector): Unit = this.coefficients = coefficients


}

abstract class FedSimulationLinRDiffLossAggregator(labelStd: Double,
                                                   labelMean: Double,
                                                   fitIntercept: Boolean,
                                                   bcCoefficients: Broadcast[Array[Double]]) extends FedDiffLossAggregator {


}
