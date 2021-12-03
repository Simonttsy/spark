package org.apache.spark.ml.fed.model.pipelinehelper

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.fed.constants.RoleType.RoleType
import org.apache.spark.ml.fed.constants.{Constants, RoleType}
import org.apache.spark.ml.fed.model.hetero.linr.HeteroLinR
import org.apache.spark.ml.fed.model.homo.HomoLR
import org.apache.spark.ml.fed.utils.ParameterTool
import org.apache.spark.ml.{Pipeline, PipelineStage}

import scala.collection.mutable.ArrayBuffer

//Todo 构建所有guest的模型，每个guest模型可以不一样，比如特征工程处理 可能不一样
// 第一guest 包含一个 Array[FedParams]


class DefaultPipelineBuilder(paramTool: ParameterTool) extends PipelineBuilder {


  override def build(role: String): FedPipeline = {

    var hasLabel: Boolean = false

    val buf = new ArrayBuffer[PipelineStage]()

    var roleId: String = null

    var roleType: RoleType = null

    var assembler: VectorAssembler = null

    val model = if (paramTool.get(Constants.ALCHEMY_PIPELINE_NAME).equalsIgnoreCase("hetero_linear_regression")) {
      println(" parse HeteroLinR param")
      new HeteroLinR()

    } else if (paramTool.get(Constants.ALCHEMY_PIPELINE_NAME).equalsIgnoreCase("homo_logistic_regression")) {
      println(" parse HomoLR param")
      new HomoLR()

    }else throw new IllegalArgumentException("can not math PIPELINE_NAME:"+paramTool.get(Constants.ALCHEMY_PIPELINE_NAME))


    model.setConfFromParamTool(paramTool)

    if (role.equalsIgnoreCase("arbiter")) {
      roleId = "1"
      roleType = RoleType.ARBITER

    } else {

      hasLabel = paramTool.get(Constants.ALCHEMY_PIPELINE_FED_LABEL).toBoolean

      roleId = paramTool.get(Constants.GUEST_ID)
      roleType = RoleType.GUEST
      val inputCols = paramTool.get(Constants.ALCHEMY_PIPELINE_FEATURE_INPUT_COLS).split(",")
      assembler = new VectorAssembler()
        .setInputCols(inputCols)
        .setOutputCol("features")
      //      .setHandleInvalid("keep")
    }

    if (assembler != null) buf += assembler
    buf += model
    val pipeline = new Pipeline().setStages(buf.toArray)

    FedPipeline(roleId, roleType, hasLabel, pipeline)

  }


}


trait PipelineBuilder {


  def build(role: String): FedPipeline
}

//Todo 去掉hasLabel，用pipeline提取
case class FedPipeline(roleId: String, roleType: RoleType, hasLabel: Boolean, pipeline: Pipeline)
