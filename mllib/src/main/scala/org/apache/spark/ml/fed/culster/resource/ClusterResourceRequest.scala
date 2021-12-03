package org.apache.spark.ml.fed.culster.resource

import org.apache.spark.ml.fed.culster.joiner.Joiner
import org.apache.spark.ml.fed.utils.ParameterTool

import scala.collection.mutable


case class ArbiterResourceRequest(hostname: String,
                                  port: String,
                                  joiner: Joiner,
                                  parameterTool: ParameterTool,
                                  sparkConfMap: mutable.Map[String, String])

//Todo 增加每个executor粒度的资源，所有资源可视化，在不需要所有节点的情况下 尽量使用资源多的executor

case class GuestResourceRequest(guestId: String,
                                hostname: String,
                                port: String,
                                parameterTool: ParameterTool,
                                sparkConfMap: mutable.Map[String, String])

//
//
//
//case class ClusterResourceRequest(hostname: String,
//                                  port: String,
//                                  sparkConfMap: mutable.Map[String, String])


//object RoleType extends Enumeration {
//
//  type RoleType = Value
//  val ARBITER, GUEST = Value
//
//}

