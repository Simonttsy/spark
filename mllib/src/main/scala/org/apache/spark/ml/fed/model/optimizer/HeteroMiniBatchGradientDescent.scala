package org.apache.spark.ml.fed.model.optimizer

import org.apache.spark.ml.fed.model.feature.FedInstance
import org.apache.spark.ml.fed.model.optimizer.Gradient.ConvergenceCheck
import org.apache.spark.ml.fed.utils.Preconditions
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

class HeteroMiniBatchGradientDescent[V](allJoinedkeys: Array[Any],
                                        fraction: Double,
                                        maxIter: Int,
                                        convergenceCheck: ConvergenceCheck[V])
  extends MiniBatchGradientDescent[V](convergenceCheck) {


  def this(keys: Array[Any],
           fraction: Double,
           maxIter: Int
          ) = {
    this(keys, fraction, maxIter, Gradient.defaultConvergenceCheck[V](maxIter))
  }

  override def replaceMeta(allInstances: RDD[FedInstance],seed: Int): Array[RDD[FedInstance]] = {
    Preconditions.checkBooleanCondition(allJoinedkeys == null, "keys of guest is empty")
    generateUnorderedBlockMeta(allJoinedkeys, fraction, seed)
      .values
      .map {
        keys =>
          val bcKeys = allInstances.context.broadcast(keys)
          allInstances.filter(instance => bcKeys.value.contains(instance.key))
      }.toArray
  }

  /** index -> Array(keys)
    * 按照比例划分kyes
    * keys 统一从 arbiter获取，避免乱序
    *
    * @param kyes
    * @param fraction
    * @tparam T
    * @return block index -> keys
    */
  def generateUnorderedBlockMeta(elems: Array[Any], fraction: Double, seed: Int): Map[Long, Array[Any]] = {

    val unordered = shuffle(elems, seed)

    val blocks = new mutable.HashMap[Long, Array[Any]]

    val totalSize = unordered.length
    var startIndex = 0

    val batchSize = math.floor(totalSize * fraction).toInt

    var untilIndex = batchSize

    var blockIndex: Long = 1

    while (untilIndex <= totalSize) {
      val block = new Array[Any](batchSize)

      var blockIndexCounter = 0
      for (i <- startIndex until untilIndex) {
        block(blockIndexCounter) = unordered(i)
        blockIndexCounter += 1
      }
      blocks.put(blockIndex, block)
      blockIndex += 1

      startIndex = untilIndex
      untilIndex += batchSize
    }

    //最后一批未被整除的
    if (totalSize > startIndex) {
      val block = new Array[Any](totalSize - startIndex)
      var blockIndexCounter = 0

      for (i <- startIndex until totalSize) {
        block(blockIndexCounter) = unordered(i)
        blockIndexCounter += 1

      }
      blocks.put(blockIndex, block)
    }

    blocks.toMap
  }


  private def shuffle(elems: Array[Any], seed: Int): Array[Any] = {

    val random = new Random(seed)
    val len = elems.length
    val arr = elems.toBuffer
    for (index <- 0 until len) {
      val RandomIndex: Int = random.nextInt(len)
      val tmp = arr(index)
      arr(index) = arr(RandomIndex)
      arr(RandomIndex) = tmp
    }
    arr.toArray
  }

}
