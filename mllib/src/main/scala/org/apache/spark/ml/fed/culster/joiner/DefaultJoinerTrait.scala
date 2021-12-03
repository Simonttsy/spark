package org.apache.spark.ml.fed.culster.joiner

import org.apache.spark.ml.fed.encryptor.Masker

trait DefaultJoinerTrait[C] extends Joiner with Masker[C]
