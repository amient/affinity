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


import java.nio.file.Paths
import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.actor.{Actor, Props}
import akka.event.Logging
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller._
import io.amient.affinity.core.actor.Gateway.{GatewayClusterStatus, GatewayConf}
import io.amient.affinity.core.actor._
import io.amient.affinity.core.config._
import io.amient.affinity.{AffinityActorSystem, Conf}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Node {

  class NodeConf extends CfgStruct[NodeConf] {
    val Containers: CfgGroup[CfgIntList] = group("container", classOf[CfgIntList], false)
    val Gateway: GatewayConf = struct("gateway", new GatewayConf)
    val StartupTimeoutMs: CfgInt = integer("startup.timeout.ms", Integer.MAX_VALUE)
    val ShutdownTimeoutMs: CfgInt = integer("shutdown.timeout.ms", 30000)
    val DataDir: CfgPath = filepath("data.dir", Paths.get("./.data")) //TODO #107 is this a reasonable default
    val SystemName: CfgString = string("name", "AffinityNode")
  }

}

class Node(config: Config) {

  val conf = Conf(config)
  private val actorSystemName: String = conf.Affi.Node.SystemName()
  val startupTimeout = conf.Affi.Node.StartupTimeoutMs().toLong milliseconds
  val shutdownTimeout = conf.Affi.Node.ShutdownTimeoutMs().toLong milliseconds

  implicit val system = AffinityActorSystem.create(actorSystemName, config)

  private val terminated = system.whenTerminated

  private val log = Logging.getLogger(system, this)

  private val controller = system.actorOf(Props(new Controller), name = "controller")

  private val httpGatewayPort = Promise[Int]()

  private val clusterReady = new CountDownLatch(1)
  system.eventStream.subscribe(system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case GatewayClusterStatus(false) => clusterReady.countDown()
    }
  })), classOf[GatewayClusterStatus])

  sys.addShutdownHook {
    if (!terminated.isCompleted) {
      log.info("process killed - attempting graceful shutdown")
      shutdown()
    }
  }

  /**
    * Await all partitions in all keyspaces to have masters
    * @return httpPort or -1 if gateway doesn't have http interface attached
    */
  def awaitClusterReady(): Unit = {
    clusterReady.await(15, TimeUnit.SECONDS)
  }

  def getHttpPort(): Int = {
    Await.result(httpGatewayPort.future, 15 seconds)
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

  def start(): Unit = {
    startContainers()
    startGateway()
  }

  def startGateway(): Unit = {
    if (conf.Affi.Node.Gateway.Class.isDefined) {
      startGateway(conf.Affi.Node.Gateway.Class().newInstance())
    } else {
      httpGatewayPort.success(-1)
    }
  }

  def startContainers(): Unit = {
    if (conf.Affi.Node.Containers.isDefined) {
      conf.Affi.Node.Containers().foreach {
        case (group: String, value: CfgIntList) =>
          val partitions = value().map(_.toInt).toList
          startContainer(group, partitions)
      }
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
    */
  def startGateway[T <: Gateway](creator: => T)(implicit tag: ClassTag[T]): Unit = {
    implicit val timeout = Timeout(startupTimeout)
    httpGatewayPort.completeWith(startupFutureWithShutdownFuse {
      controller ack CreateGateway(Props(creator))
    })
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
