package org.apache.spark.ml.fed.model.optimizer

import org.apache.spark.ml.fed.model.loss.DiffFunction
import org.apache.spark.ml.fed.model.optimizer.Gradient.ConvergenceCheck

private[fed] class LBFGS[V](batchFraction: Double = 0.1,
                            epoch: Long = 20, //需要打乱每次data顺序
                            convergenceCheck: ConvergenceCheck[V]) extends Gradient[V, DiffFunction[V]](convergenceCheck) {
  /**
    * Any history the derived minimization function needs to do its updates. typically an approximation
    * to the second derivative/hessian matrix.
    */

  override type History = this.type


}
