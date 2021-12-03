package org.apache.spark.ml.fed.event

private[fed]
class PartyLossReason (val message: String) extends Serializable {
  override def toString: String = message
}