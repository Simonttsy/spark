package org.apache.spark.ml.fed.utils

abstract class HashParms {


  def $(v: Any): Option[Int] = {
    if (v != null && v.toString.nonEmpty) Some(v.toString.hashCode)
    else None
  }

//  def equals(thisOne: Any, otherOne: Any): Boolean = {
//
//    if (thisOne != null && thisOne.toString.nonEmpty && otherOne != null && otherOne.toString.nonEmpty) {
//      thisOne.toString.hashCode == otherOne.toString.hashCode
//    } else false
//  }
}

