package org.apache.spark.ml.fed.model.base

import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.fed.utils.ParameterTool
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

trait FederatedBase {

  def initialize(dataset: Dataset[_]): RDD[FedInstance]

  def initializeLossAggregatorAndoptimizer(initialCoef: Vector)

  def setConfFromParamTool(paramTool: ParameterTool)

  def fedFit()

}
