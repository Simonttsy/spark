package org.apache.spark.ml.fed.model.optimizer

import breeze.util.Implicits._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.fed.model.loss.DiffFunction
import org.apache.spark.ml.fed.model.optimizer.Gradient.{ConvergenceCheck, ConvergenceReason}

/**
  *
  * @param convergenceCheck
  * @tparam V    vec type
  * @tparam DF   diff type
  * @tparam DATA examples type
  */
abstract class Gradient[V, DF <: DiffFunction[V]
//DF <: DiffFunction[C, RDD[FedInstance[C]]]
](val convergenceCheck: ConvergenceCheck[V]) extends Logging {

  /**
    * Any history the derived minimization function needs to do its updates. typically an approximation
    * to the second derivative/hessian matrix.
    */
  type History
  //  type State = Gradient.State[V, History]
  type State = Gradient.State[V]

  def adjustFunction(f: DF): DF = f

  def adjust(newX: V, newGrad: V, newVal: Double): (Double, V) = (newVal, newGrad)

  //  def initialHistory(f: DF, init: T): History

  def calculateObjective(f: DF, w: V): (Double, V) = {
    f.calculate(w)
  }

  //  def updateHistory(newX: T, newGrad: T, newVal: Double, f: DF, oldState: State): History

//  def unpersist():Unit

  protected def initialState(f: DF, init: V): State = {

    val w = init
    //    val history = initialHistory(f, init)
    //    val (value, grad) = calculateObjective(f, x)
    val (value, coef) = calculateObjective(f, w)

    //    val (adjValue, adjGrad) = adjust(x, grad, value)
    println(1, "initialize state w = " + w)
    Gradient.State(coef, value, 1)
  }

  def infiniteIterations(f: DF, state: State): Iterator[State] = {
    var failedOnce = false
    val adjustedFun = adjustFunction(f)

    Iterator.iterate(state) { state =>
      try {
        val w = state.w
        println(state.epoch + 1, " state w = " + w)

        //        val dir = chooseDescentDirection(state, adjustedFun)
        //        val stepSize = determineStepSize(state, adjustedFun, dir)
        //        logInfo(f"Step Size: $stepSize%.4g")
        //        val x = takeStep(state, dir, stepSize)
        val (value, coef) = calculateObjective(adjustedFun, w)
        //        val (adjValue, adjGrad) = adjust(w, grad, value)
        //        val oneOffImprovement = (state.adjustedValue - adjValue) / (state.adjustedValue.abs
        //          .max(adjValue.abs)
        //          .max(1E-6 * state.initialAdjVal.abs))
        //        logInfo(f"Val and Grad Norm: $adjValue%.6g (rel: $oneOffImprovement%.3g) ${norm(adjGrad)}%.6g")
        //        val history = updateHistory(w, grad, value, adjustedFun, state)
        failedOnce = false

        //Todo update blocks meta
        Gradient.State(
          w,
          value,
          //          grad,
          //          adjValue,
          //          adjGrad,
          state.epoch + 1,
          //          state.initialAdjVal
        )
      } catch {
        case x: RuntimeException if !failedOnce =>
          failedOnce = true
          logError(s"Failure! Resetting history! Due to ${x.getMessage}")
          //          state.copy(history = initialHistory(adjustedFun, state.w))
          state
        case x: RuntimeException =>
          //          logError(s"Failure again! Due to${x.getMessage} !Giving up and returning. Maybe the objective is just poorly behaved?")
          //          state.copy(searchFailed = true)
          state
      }
    }
  }


  /**
    * iteration epoch,batch gradient in calculate
    *
    * @param f
    * @param init
    * @param w
    * @return
    */
  def iterations(f: DF, w: V): Iterator[State] = {
    val adjustedFun = adjustFunction(f)
    infiniteIterations(f, initialState(adjustedFun, w)).takeUpToWhere { s =>
      convergenceCheck.apply(s) match {
        case Some(converged) =>
          logInfo(s"Converged because ${converged.reason}")
//          s.convergenceReason = Some(converged)
          true
        case None =>
          false
      }
    }
  }



  def convergence(s: State): Option[ConvergenceReason] ={
    convergenceCheck.apply(s) match {
      case Some(converged) =>
        logInfo(s"Converged because ${converged.reason}")
        Some(converged)
      case None =>
        None
    }
  }
}

object Gradient {

  /**
    * Tracks the information about the optimizer, including the current point, its value, gradient, and then any history.
    * Also includes information for checking convergence.
    *
    * @param w                 the current point being considered
    * @param value             f(x)
    * @param grad              f.gradientAt(x)
    * @param adjustedValue     f(x) + r(x), where r is any regularization added to the objective. For LBFGS, this is f(x).
    * @param adjustedGradient  f'(x) + r'(x), where r is any regularization added to the objective. For LBFGS, this is f'(x).
    * @param epoch             what iteration number on the entire data set.
    * @param initialAdjVal     f(x_0) + r(x_0), used for checking convergence
    * @param history           any information needed by the optimizer to do updates.
    * @param searchFailed      did the line search fail?
    * @param convergenceReason the convergence reason
    */
  case class State[+V](w: V, //guest本地w，arbiter为空或加合
                       value: Double, //loss_norm + (reg)
                       //                                 grad: V,
                       //                                 adjustedValue: Double,
                       //                                 adjustedGradient: V,
                       epoch: Long //遍历RDD次数，非batch次数
                       //                                 initialAdjVal: Double, //初始化loss？
                       //                                 //                                 history: History, //可以理解成累加器而State是记录每次状态
                       //                                 searchFailed: Boolean = false,
//                       var convergenceReason: Option[ConvergenceReason] = None
                      )


  trait ConvergenceCheck[V] {

    def apply(state: State[V]): Option[ConvergenceReason]

    //    def ||(otherCheck: ConvergenceCheck[T]): ConvergenceCheck[T] = orElse(otherCheck)

    //    def orElse(other: ConvergenceCheck[T]): ConvergenceCheck[T] = {
    //      SequenceConvergenceCheck(asChecks ++ other.asChecks)
    //    }

  }

  object ConvergenceCheck {
//    implicit
    def fromPartialFunction[V](pf: PartialFunction[State[V], ConvergenceReason]): ConvergenceCheck[V] =
    //      (state: State[V, _]) => pf.lift(state)
      (state: State[V]) => pf.lift(state)

  }

  trait ConvergenceReason {
    def reason: String
  }

  case object MaxIterations extends ConvergenceReason {
    override def reason: String = "max iterations reached"
  }

  case object FunctionValuesConverged extends ConvergenceReason {
    override def reason: String = "function values converged"
  }

  case object GradientConverged extends ConvergenceReason {
    override def reason: String = "gradient converged"
  }

  case object SearchFailed extends ConvergenceReason {
    override def reason: String = "line search failed!"
  }

  case object MonitorFunctionNotImproving extends ConvergenceReason {
    override def reason: String = "monitor function is not improving"
  }

  case object ProjectedStepConverged extends ConvergenceReason {
    override def reason: String = "projected step converged"
  }
//  ConvergenceCheck.fromPartialFunction()
  def maxIterationsReached[V](maxEpoch: Int): ConvergenceCheck[V] = ConvergenceCheck.fromPartialFunction {
    case s: State[_] if s.epoch >= maxEpoch && maxEpoch >= 0 =>
      MaxIterations
  }


  //Todo 增加 tolerance 等
  def defaultConvergenceCheck[V](maxIter: Int): ConvergenceCheck[V] = maxIterationsReached[V](maxIter)

}
