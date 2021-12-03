package org.apache.spark.ml.fed.model.param

import org.apache.spark.ml.fed.constants.RoleType.RoleType
import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.fed.utils.{Preconditions, Utils}
import org.apache.spark.ml.functions.checkNonNegativeWeight
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasWeightCol}
import org.apache.spark.ml.param.{DoubleArrayParam, Param, Params}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, Row}

trait FedParams extends HasWeightCol with HasLabelCol with HasFeaturesCol
  with HasPrimarykey with HasFedLabel with HasMask with HasEncryptor with HasRoleType with GuestId with HasTrainingSetRatio{

  //Todo 检查初始化参数是否合规,是否有 optimizer
  def checkInvalidParams(check: this.type => Boolean): Unit = {
    Preconditions.checkBooleanCondition(check(this), "Invalid parameters")
  }

  def extractFedInstances(dataset: Dataset[_]): RDD[FedInstance] = {

    val w = this match {
      case p: HasWeightCol =>
        if (isDefined(p.weightCol) && $(p.weightCol).nonEmpty) {
          println("checkNonNegativeWeight")
          checkNonNegativeWeight((col($(p.weightCol)).cast(DoubleType)))
        } else {
          println("litlitlit")
          lit(1.0)
        }
    }

    //Todo 优化FedInstance
    if (isDefined(primarykeyCol)) {
      println("primarykeyCol")
      println("w.toString, $(primarykeyCol), $(fedLabel), $(featuresCol)")
      println(w.toString, $(primarykeyCol), $(fedLabel), $(featuresCol))
      if ($(fedLabel)) {
        println("fedLabel")
        dataset.select(col($(primarykeyCol)), col($(labelCol)).cast(DoubleType), w, col($(featuresCol))).rdd.map {
          case Row(pk: Any, label: Double, weight: Double, features: Vector) =>
            FedInstance(pk, label, weight, features)
        }
      } else { //Todo 做成LabelFedInstance / CommonFedInstance
        println("no fedLabel")
        dataset.select(col($(primarykeyCol)), w, col($(featuresCol))).rdd.map {
          case Row(pk: Any, weight: Double, features: Vector) =>
            FedInstance(pk, Double.NaN, weight, features)
        }
      }
    }else{
      println("transform homo df")
      dataset.select(col($(labelCol)).cast(DoubleType),w, col($(featuresCol))).rdd.map {
        case Row(label: Double,weight: Double, features: Vector) =>
          FedInstance(null, label, weight, features)
      }
    }
  }

}


/**
  * Trait for common param coefficients.
  */
trait HasCoefficients extends Params {

  //Todo 增加 适合coefficients 的randmon函数
  /**
    * Param for coefficients ,If this is not set or empty, we treat all features weights random
    *
    * @group param
    */
  final val coefficients: DoubleArrayParam = new DoubleArrayParam(this, "coefficients", "instances coefficients", (t: Array[Double]) => !t.contains(0))

  //  setDefault(coefficients->Array.empty)

  /** @group getParam */
  final def getCoefficients: Array[Double] = $(coefficients)
}


trait HasPrimarykey extends Params {

  /**
    * Param for primary key column name.
    *
    * @group param
    */
  final val primarykeyCol: Param[String] = new Param[String](this, "primarykeyCol", "primary key column name")


  /** @group getParam */
  final def getPrimarykeyCol: String = $(primarykeyCol)
}

/**
  * 后面版本打算弃用
  */
@deprecated
trait HasSingleLabelSideId extends Params {

  /**
    * id for just one guest which has predict label
    */
  final val singleLabelSideId: Param[String] = new Param[String](this, "label side id", "The label side id")

  /** @group getParam */
  final def getSingleLabelSideId: String = $(singleLabelSideId)
}


trait HasFedLabel extends Params {

  /**
    * label for label side
    */
  final val fedLabel: Param[Boolean] = new Param[Boolean](this, "fed label", "label for label side")

  /** @group getParam */
  final def getFedLabel: Boolean = $(fedLabel)
}

/**
  * 需要保证多方数据量一致!!!!
  */
trait HasBatchFraction extends Params {

  final val batchFraction: Param[Double] = new Param[Double](this, "batch fraction", "Param for batch fraction that stacking input data into blocks")

  /** @group getParam */
  final def getBatchFraction: Double = $(batchFraction)
}

//Todo 增加按块数shuffle参数


trait HasMask extends Params {

  final val mask: Param[Double] = new Param[Double](this, "mask", "mask")

  setDefault(mask -> Utils.defaultMask())

  /** @group getParam */
  final def getMask: Double = $(mask)
}

trait HasRegLambda extends Params {

  /**
    * Param for scale the regularization
    */
  final val regLambda: Param[Double] = new Param[Double](this, "reg lambda", "scale the regularization")

  setDefault(regLambda -> 0.02)

  /** @group getParam */
  final def getRegLambda: Double = $(regLambda)
}

trait HasEncryptor extends Params {

  /**
    * Param for encrypt
    */
  final val encryptor: Param[Encryptor] = new Param[Encryptor](this, "encryptor", "Encryptor")

  /** @group getParam */
  final def getEncryptor: Encryptor = $(encryptor)
}


trait HasRoleType extends Params {

  /**
    * arbiter or guest
    */
  final val roleType: Param[RoleType] = new Param[RoleType](this, "role type", "role type")

  /** @group getParam */
  final def getRoleType: RoleType = $(roleType)
}

trait GuestId extends Params {

  /**
    * uniquely for guest id
    */
  final val guestId: Param[String] = new Param[String](this, "guest id", "guest id")

  /** @group getParam */
  final def getGuestId: String = $(guestId)
}

trait HasTrainingSetRatio extends Params {

  final val trainingSetRatio: Param[Double] = new Param[Double](this, "training set ratio", "training set ratio")

  setDefault(trainingSetRatio -> 0.7)

  /** @group getParam */
  final def getTrainingSetRatio: Double = $(trainingSetRatio)
}
