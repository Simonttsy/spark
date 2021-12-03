package org.apache.spark.ml.fed.utils

object Preconditions {


  // ------------------------------------------------------------------------
  //  Null checks
  // ------------------------------------------------------------------------
  /**
    * Ensures that the given object reference is not null. Upon violation, a {@code
    * NullPointerException} with no message is thrown.
    *
    * @param reference The object reference
    * @return The object reference itself (generically typed).
    * @throws NullPointerException Thrown, if the passed reference was null.
    */
  def checkNotNull[T](reference: T): T = {
    if (reference == null) throw new NullPointerException
    reference
  }


  /**
    * Ensures that the given object reference is not null. Upon violation, a {@code
    * NullPointerException} with the given message is thrown.
    *
    * @param reference    The object reference
    * @param errorMessage The message for the { @code NullPointerException} that is thrown if the
    *                                                 check fails.
    * @return The object reference itself (generically typed).
    * @throws NullPointerException Thrown, if the passed reference was null.
    */
  def checkNotNull[T](reference: T, errorMessage: String): T = {
    if (reference == null) throw new NullPointerException(String.valueOf(errorMessage))
    reference
  }


//  /**
//    * Ensures that the given object reference is not null. Upon violation, a {@code
//    * NullPointerException} with the given message is thrown.
//    *
//    * <p>The error message is constructed from a template and an arguments array, after a similar
//    * fashion as {@link String#format(String, Object...)}, but supporting only {@code %s} as a
//    * placeholder.
//    *
//    * @param reference            The object reference
//    * @param errorMessageTemplate The message template for the { @code NullPointerException} that is
//    *                                                                  thrown if the check fails. The template substitutes its { @code %s} placeholders with the
//    *                                                                  error message arguments.
//    * @param errorMessageArgs The arguments for the error message, to be inserted into the message
//    *                         template for the { @code %s} placeholders.
//    * @return The object reference itself (generically typed).
//    * @throws NullPointerException Thrown, if the passed reference was null.
//    */
//  def checkNotNull[T](reference: T,  errorMessageTemplate: String, errorMessageArgs: Any*): T = {
//    if (reference == null) throw new NullPointerException(format(errorMessageTemplate, errorMessageArgs))
//    reference
//  }




  def checkNotNone[T](reference: Option[T]): Option[T] = {
    if (reference.isEmpty) throw new NullPointerException
    reference
  }


  def checkNotNone[T](reference: Option[T], errorMessage: String): Option[T] = {
    if (reference.isEmpty) throw new NullPointerException(String.valueOf(errorMessage))
    reference
  }


  def checkBooleanCondition[T](reference: Boolean, errorMessage: String): Unit = {
    if (reference) throw new Exception(String.valueOf(errorMessage))
  }
}
