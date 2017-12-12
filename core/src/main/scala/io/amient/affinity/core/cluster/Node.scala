/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.core.cluster


import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigException, ConfigFactory, ConfigList}
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller._
import io.amient.affinity.core.actor.Service.ServiceConfig
import io.amient.affinity.core.actor._
import io.amient.affinity.core.config._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Node {

  final val CONFIG_NODE_CONTAINERS = "affinity.node.container"
  final def CONFIG_SERVICE(group: String) = s"affinity.service.$group"
  final val CONFIG_NODE_STARTUP_TIMEOUT_MS = "affinity.node.startup.timeout.ms"
  final val CONFIG_NODE_SHUTDOWN_TIMEOUT_MS = "affinity.node.shutdown.timeout.ms"
  final val CONFIG_NODE_SYSTEM_NAME = "affinity.node.name"
  final val CONFIG_AKKA_HOST = "akka.remote.netty.tcp.hostname"
  final val CONFIG_AKKA_PORT = "akka.remote.netty.tcp.port"
  final val CONFIG_DATA_DIR = "affinity.node.data.dir"

  object Config extends ConfigStruct("affinity.node") {
    val ConfigStartupTimeoutMs = required(new ConfigLong("startup.timeout.ms"))
    val ConfigNodeShutdownTimeoutMs = required(new ConfigLong("shutdown.timeout.ms"))
    val ConfigServices = optional(new ConfigGroup("service", classOf[ServiceConfig]))
    //property[Long]("", classOf[Long], false)
//    property[String]("container", classOf[String])
//    group("affinity.service", classOf[ServiceConfig])
    //group("affiniyt.container")
  }


}

class Node(config: Config) {

  import Node._

  private val actorSystemName = config.getString(CONFIG_NODE_SYSTEM_NAME)
  val startupTimeout = config.getInt(CONFIG_NODE_STARTUP_TIMEOUT_MS) milliseconds
  val shutdownTimeout = config.getInt(CONFIG_NODE_SHUTDOWN_TIMEOUT_MS) milliseconds

  implicit val system = ActorSystem.create(actorSystemName, config)

  private val controller = system.actorOf(Props(new Controller), name = "controller")

  sys.addShutdownHook {
    //in case the process is stopped from outside
    shutdown()
  }

  final def shutdown(): Unit = {
    controller ! GracefulShutdown()
    Await.ready(system.whenTerminated, shutdownTimeout)
  }

  import system.dispatcher

  implicit val scheduler = system.scheduler

  implicit def partitionCreatorToProps[T <: Partition](creator: => T)(implicit tag: ClassTag[T]): Props = {
    Props(creator)
  }

  def start() = {
    if (config.hasPath(CONFIG_NODE_CONTAINERS)) {
      config.getObject(CONFIG_NODE_CONTAINERS).asScala.foreach { case (group, value) =>
        val partitions = value.asInstanceOf[ConfigList].asScala.map(_.unwrapped().asInstanceOf[Int]).toList
        startContainer(group, partitions)
      }
    }

    if (config.hasPath(GatewayHttp.CONFIG_GATEWAY_CLASS)) {
      val cls = Class.forName(config.getString(GatewayHttp.CONFIG_GATEWAY_CLASS)).asSubclass(classOf[ServicesApi])
      startGateway(cls.newInstance())
    }
  }

  def startContainer(group: String, partitions: List[Int]): Future[Unit] = {
    try {
      val serviceConfig = config.getConfig(CONFIG_SERVICE(group))
      val serviceClass = Class.forName(serviceConfig.getString(Service.CONFIG_SERVICE_CLASS)).asSubclass(classOf[Partition])
      implicit val timeout = Timeout(startupTimeout)
      startupFutureWithShutdownFuse(controller ack CreateContainer(group, partitions, Props(serviceClass.newInstance())))
    } catch {
      case e: ConfigException =>
        throw new IllegalArgumentException(s"Could not start container for service $group with partitions ${partitions.mkString(", ")}", e)
    }
  }

  def startContainer[T <: Partition](group: String, partitions: List[Int], partitionCreator: => T)
                                    (implicit tag: ClassTag[T]): Future[Unit] = {
    implicit val timeout = Timeout(startupTimeout)
    startupFutureWithShutdownFuse(controller ack CreateContainer(group, partitions, Props(partitionCreator)))
  }

  /**
    * @param creator
    * @param tag
    * @tparam T
    * @return the httpPort on which the gateway is listening
    */
  def startGateway[T <: ServicesApi](creator: => T)(implicit tag: ClassTag[T]): Future[Int] = {
    implicit val timeout = Timeout(startupTimeout)
    startupFutureWithShutdownFuse {
      controller ack CreateGateway(Props(creator))
    }
  }

  private def startupFutureWithShutdownFuse[T](eventual: Future[T]): Future[T] = {
    eventual onFailure {
      case NonFatal(e) =>
        e.printStackTrace()
        shutdown()
    }
    eventual
  }


}
