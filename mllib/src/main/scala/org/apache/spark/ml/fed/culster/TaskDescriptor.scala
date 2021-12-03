package org.apache.spark.ml.fed.culster

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.ml.fed.culster.joiner.Joiner
import org.apache.spark.ml.fed.encryptor.Encryptor
import org.apache.spark.ml.fed.scheduler.GuestData

class TaskDescriptor(val id: String,
                     val primaryKey: String,
                     val encryptor: Encryptor,
                     val joiner: Joiner,
                     val aggrSideId:String,
                     val guestDataMap: ConcurrentHashMap[String, GuestData]) extends Serializable {

  def encode(taskDescription: TaskDescriptor): ByteBuffer = {
    null
  }
}
