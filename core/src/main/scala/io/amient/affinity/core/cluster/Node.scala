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
import com.typesafe.config.{Config, ConfigException}
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller._
import io.amient.affinity.core.actor._
import io.amient.affinity.core.config._

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Node {

  object Config extends Config

  class Config extends CfgStruct[Config](Cfg.Options.IGNORE_UNKNOWN) {
    val Akka = struct("akka.remote", new RemoteConfig, false)
    val Node = struct("affinity.node", new NodeConfig, true)
    val Services = struct(new ServicesApi.Config, false)
  }

  class RemoteConfig extends CfgStruct[RemoteConfig](Cfg.Options.IGNORE_UNKNOWN) {
    val Hostname = string("netty.tcp.hostname", true)
    val Port = integer("netty.tcp.port", true)
  }


  class NodeConfig extends CfgStruct[NodeConfig] {
    val Containers = group("container", classOf[CfgIntList], false)
    val Gateway = struct("gateway", new GatewayHttp.Conf, false)
    val StartupTimeoutMs = longint("startup.timeout.ms", true)
    val ShutdownTimeoutMs = longint("shutdown.timeout.ms", true)
    val DataDir = string("data.dir", true)
    val SystemName = string("name", true)
  }

}

class Node(config: Config) {

  val appliedConfig = Node.Config(config)
  private val actorSystemName: String = appliedConfig.Node.SystemName()
  val startupTimeout = appliedConfig.Node.StartupTimeoutMs().toLong milliseconds
  val shutdownTimeout = appliedConfig.Node.ShutdownTimeoutMs().toLong milliseconds

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
    appliedConfig.Node.Containers().foreach {
      case (group: String, value: CfgIntList) =>
        val partitions = value().map(_.toInt).toList
        startContainer(group, partitions)
    }
    if (config.hasPath(GatewayHttp.CONFIG_GATEWAY_CLASS)) {
      val cls = Class.forName(config.getString(GatewayHttp.CONFIG_GATEWAY_CLASS)).asSubclass(classOf[ServicesApi])
      startGateway(cls.newInstance())
    }
  }

  def startContainer(group: String, partitions: List[Int]): Future[Unit] = {
    try {
      val serviceClass = appliedConfig.Services.Services(group).PartitionClass()
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
