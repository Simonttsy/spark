package org.apache.spark.ml.fed.event

import org.apache.spark.ml.fed.culster.TaskDescriptor
import org.apache.spark.rpc.RpcEndpointRef

private[fed] sealed trait ClusterMessages extends Serializable

private[fed] object ClusterMessages {

  case object RegisteredGuest extends ClusterMessages

  case class RegisterGuest(
                            guestId: String,
//                            hostname: String,
                            guestRef: RpcEndpointRef
                            //                               cores: Int,
                            //                               logUrls: Map[String, String],
                            //                               attributes: Map[String, String],
                            //                               resources: Map[String, ResourceInformation],
                          )
    extends ClusterMessages


  case class RemoveParty(guestId: String, reason: PartyLossReason) extends ClusterMessages

  case object ReviveTask extends ClusterMessages

  case class LaunchTask(guestId:String) extends ClusterMessages

//  case class SubmitTask(data: SerializableBuffer) extends ClusterMessages
  case class SubmitTask(task: TaskDescriptor) extends ClusterMessages

  case class JoinKey[C](guestId: String,codedValues:Array[C]) extends ClusterMessages

  case class JoinComplete(codedValues:Array[Any]) extends ClusterMessages

  case class AlreadyFits(guestId: String) extends ClusterMessages
}
