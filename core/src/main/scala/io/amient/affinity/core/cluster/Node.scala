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
import akka.event.Logging
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller._
import io.amient.affinity.core.actor.Gateway.GatewayConf
import io.amient.affinity.core.actor.Keyspace.KeyspaceConf
import io.amient.affinity.core.actor._
import io.amient.affinity.core.config._
import io.amient.affinity.core.storage.StateConf

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Node {

  object Conf extends Conf {
    override def apply(config: Config): Conf = new Conf().apply(config)
  }

  class Conf extends CfgStruct[Conf](Cfg.Options.IGNORE_UNKNOWN) {
    val Akka = struct("akka", new AkkaConf, false)
    val Affi = struct("affinity", new AffinityConf, true)
  }

  class AkkaConf extends CfgStruct[AkkaConf](Cfg.Options.IGNORE_UNKNOWN) {
    val Hostname = string("remote.netty.tcp.hostname", false)
    val Port = integer("remote.netty.tcp.port", false)
  }


  class AffinityConf extends CfgStruct[AffinityConf] {
    val Avro = struct("avro", new AvroConf(), true)
    val Coorinator = struct("coordinator", new Coordinator.CoorinatorConf, true)
    val Keyspace = group("keyspace", classOf[KeyspaceConf], false)
    val Global = group("global", classOf[StateConf], false)
    val Containers = group("node.container", classOf[CfgIntList], false)
    val Gateway = struct("node.gateway", new GatewayConf, false)
    val StartupTimeoutMs = integer("node.startup.timeout.ms", true)
    val ShutdownTimeoutMs = integer("node.shutdown.timeout.ms", true)
    val DataDir = filepath("node.data.dir", false)
    val SystemName = string("node.name", true)
  }

}

class Node(config: Config) {

  val conf = Node.Conf(config)
  private val actorSystemName: String = conf.Affi.SystemName()
  val startupTimeout = conf.Affi.StartupTimeoutMs().toLong milliseconds
  val shutdownTimeout = conf.Affi.ShutdownTimeoutMs().toLong milliseconds

  implicit val system = ActorSystem.create(actorSystemName, config)

  private val terminated = system.whenTerminated

  private val log = Logging.getLogger(system, this)

  private val controller = system.actorOf(Props(new Controller), name = "controller")

  sys.addShutdownHook {
    if (!terminated.isCompleted) {
      log.info("process killed - attempting graceful shutdown")
      shutdown()
    }
  }

  final def shutdown(): Unit = {
    controller ! GracefulShutdown()
    Await.ready(terminated, shutdownTimeout)
  }

  import system.dispatcher

  implicit val scheduler = system.scheduler

  implicit def partitionCreatorToProps[T <: Partition](creator: => T)(implicit tag: ClassTag[T]): Props = {
    Props(creator)
  }

  def start() = {
    conf.Affi.Containers().foreach {
      case (group: String, value: CfgIntList) =>
        val partitions = value().map(_.toInt).toList
        startContainer(group, partitions)
    }
    if (conf.Affi.Gateway.Class.isDefined) {
      //the gateway could be started programatically but in this case it is by config
      startGateway(conf.Affi.Gateway.Class().newInstance())
    }
  }

  def startContainer(group: String, partitions: List[Int]): Future[Unit] = {
    try {
      val serviceClass = conf.Affi.Keyspace(group).PartitionClass()
      implicit val timeout = Timeout(startupTimeout)
      startupFutureWithShutdownFuse(controller ack CreateContainer(group, partitions, Props(serviceClass.newInstance())))
    } catch {
      case NonFatal(e) =>
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
  def startGateway[T <: Gateway](creator: => T)(implicit tag: ClassTag[T]): Future[Int] = {
    implicit val timeout = Timeout(startupTimeout)
    startupFutureWithShutdownFuse {
      controller ack CreateGateway(Props(creator))
    }
  }

  private def startupFutureWithShutdownFuse[T](eventual: Future[T]): Future[T] = {
    eventual onFailure {
      case NonFatal(e) =>
        log.error(e, "Could not execute startup command")
        shutdown()
    }
    eventual
  }


}
