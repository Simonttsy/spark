package org.apache.spark.ml.fed.constants

object Constants {

  /**
    * config on common
    */

  val ALCHEMY_ROLE = "alchemy.role"

  /**
    * config on arbiter
    */

  val ALCHEMY_PRIMARY_KEY = "alchemy.primary.key"

  val ALCHEMY_JOINER: String = "alchemy.joiner"

  /**
    * config on guest
    */

  val GUEST_ID: String = "alchemy.guest.id"

  val GUEST_ON_ARBITER = "alchemy.guest.on.arbiter"

  val GUEST_DRIVER_HOSTNAME: String = "alchemy.guest.driver.hostname"

  val ARBITER_DRIVER_HOSTNAME = "alchemy.arbiter.driver.hostname"

  val ARBITER_DRIVER_PORT = "alchemy.arbiter.driver.port"

  val ALCHEMY_PIPELINE_NAME = "alchemy.pipeline.name"
  val ALCHEMY_PIPELINE_COEFFICIENTS = "alchemy.pipeline.coefficients"
  val ALCHEMY_PIPELINE_TRAINING_SET_RATIO = "alchemy,pipeline.training.set.ratio"
  val ALCHEMY_PIPELINE_INTERCEPT = "alchemy.pipeline.intercept"
  val ALCHEMY_PIPELINE_REGULARIZATION = "alchemy.pipeline.regularization"
  val ALCHEMY_PIPELINE_SCALE_REGULARIZATION = "alchemy.pipeline.scale.regularization"
  val ALCHEMY_PIPELINE_STEP_SIZE = "alchemy.pipeline.step.size"
  val ALCHEMY_PIPELINE_FED_LABEL = "alchemy.pipeline.fed.label"
  val ALCHEMY_PIPELINE_MAX_ITER = "alchemy.pipeline.max.iter"
  val ALCHEMY_PIPELINE_BATCH_FRACTION = "alchemy.pipeline.batch.fraction"

  val ALCHEMY_PIPELINE_FEATURE_INPUT_COLS = "alchemy.pipeline.feature.input.cols"

  val ALCHEMY_PIPELINE_MODEL_SAVE_DIR = "alchemy.pipeline.model.save.dir"

  val ALCHEMY_PIPELINE_MODEL_SAVE_FILE_NAME = "alchemy.pipeline.model.save.file.name"

  /**
    * spark
    */

  val SPARK_MASTER: String = "spark.master"

  val SPARK_DRIVER_PORT: String = "spark.driver.port"

  /**
    * Connector
    */

  val SOURCE_TYPE: String = "alchemy.source.type"

  val SOURCE_CSV_PATH: String = "alchemy.source.csv.path"


  val SOURCE_JDBC_URL: String = "alchemy.source.jdbc.url"
  val SOURCE_JDBC_TABLE: String = "alchemy.source.jdbc.table"
  val SOURCE_JDBC_USER: String = "alchemy.source.jdbc.user"
  val SOURCE_JDBC_PASSWORD: String = "alchemy.source.jdbc.password"

}


object RoleType extends Enumeration {

  type RoleType = Value

  val ARBITER, GUEST = Value

}