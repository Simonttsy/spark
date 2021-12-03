package org.apache.spark.ml.fed.encryptor

import org.apache.spark.ml.fed.culster.joiner.Joiner
import org.apache.spark.ml.fed.culster.masker.DefaultMasker

class TestVarJoiner  extends DefaultMasker(3.3) with Joiner{
  override def innerJoin(thisSeq: Array[Any], otherSeq: Array[Any]): Array[Any] = ???
}
