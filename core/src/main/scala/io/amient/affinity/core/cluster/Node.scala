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
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller._
import io.amient.affinity.core.actor.Gateway.{GatewayClusterStatus, GatewayConf}
import io.amient.affinity.core.actor._
import io.amient.affinity.core.config._
import io.amient.affinity.{AffinityActorSystem, Conf}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag

object Node {

  class NodeConf extends CfgStruct[NodeConf] {
    val Containers: CfgGroup[CfgIntList] = group("container", classOf[CfgIntList], false)
      .doc("Array of partitions assigned to this node, <ID> represents the Keyspace, e.g. assigning first four partitions of MyKeySpace: affinity.node.container.MyKeySpace = [0,1,2,3] ")
    val Gateway: GatewayConf = struct("gateway", new GatewayConf, false)
    val StartupTimeoutMs = longint("startup.timeout.ms", Integer.MAX_VALUE).doc("Maximum time a node can take to startup - this number must account for any potential state bootstrap")
    val ShutdownTimeoutMs = longint("shutdown.timeout.ms", 30000).doc("Maximum time a node can take to shutdown gracefully")
    //TODO #107 is this a reasonable default
    val DataDir = filepath("data.dir", Paths.get("./.data")).doc("Location under which any local state or registers will be kept")
    val SystemName = string("name", "Affinity").doc("ActorSystem name under which the Node presents itself in the Akka Cluster")
  }

}

class Node(config: Config) {

  val conf = Conf(config)
  private val actorSystemName: String = conf.Affi.Node.SystemName()
  val startupTimeout = conf.Affi.Node.StartupTimeoutMs().toLong milliseconds
  val shutdownTimeout = conf.Affi.Node.ShutdownTimeoutMs().toLong milliseconds

  implicit val system = AffinityActorSystem.create(actorSystemName, config)

  private val log = LoggerFactory.getLogger(this.getClass) //Logging.getLogger(system, this)

  private val controller = system.actorOf(Props(new Controller), name = "controller")

  private val httpGatewayPort = Promise[List[Int]]()

  private val clusterReady = new CountDownLatch(1)

  @volatile private var shuttingDown = false

  @volatile private var fatalError: Option[Throwable] = None

  import scala.concurrent.ExecutionContext.Implicits.global

  val systemEventsWatcher = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case GatewayClusterStatus(false) => clusterReady.countDown()
      case FatalErrorShutdown(e) =>
        fatalError = Some(e)
        shutdown()
    }
  }))

  system.eventStream.subscribe(systemEventsWatcher, classOf[GatewayClusterStatus])

  system.eventStream.subscribe(systemEventsWatcher, classOf[FatalErrorShutdown])

  sys.addShutdownHook {
    if (!shuttingDown) {
      log.info("process killed - attempting graceful shutdown")
      fatalError = None
      shutdown()
    }
    Await.ready(system.terminate, shutdownTimeout)
  }

  /**
    * Await all partitions in all keyspaces to have masters
    *
    * @return httpPort or -1 if gateway doesn't have http interface attached
    */
  def awaitClusterReady(): Unit = {
    clusterReady.await(startupTimeout.toMillis, TimeUnit.MILLISECONDS)
  }

  def getHttpPort(interface: Int): Int = {
    Await.result(httpGatewayPort.future, 15 seconds)(interface)
  }

  final def shutdown(): Unit = if (!shuttingDown) {
    shuttingDown = true
    Await.result(fatalError.map { e =>
      log.error("Affinity Fatal Error", e)
      Future.successful(System.exit(1))
    }.getOrElse {
      system.terminate()
    }, shutdownTimeout)
    log.info("Node shutdown completed")
  }

  implicit def partitionCreatorToProps[T <: Partition](creator: => T)(implicit tag: ClassTag[T]): Props = {
    Props(creator)
  }

  def start(): Unit = try {
    Await.result(Future.sequence(startGateway() +: startContainers()), startupTimeout)
  } catch {
    case e: Throwable =>
      Await.result(system.terminate(), shutdownTimeout)
      throw e
  }

  /**
    * @return a future with list of ports indexed by the interface id 0-n
    */
  def startGateway(): Future[List[Int]] = {
    if (conf.Affi.Node.Gateway.Class.isDefined) {
      startGateway(conf.Affi.Node.Gateway.Class().newInstance())
    } else {
      httpGatewayPort.success(List())
      Future.successful(List())
    }
  }

  /**
    * @param creator
    * @param tag
    * @tparam T
    * @return a future with list of ports indexed by the interface id 0-n
    */
  def startGateway[T <: Gateway](creator: => T)(implicit tag: ClassTag[T]): Future[List[Int]] = {
    implicit val timeout = Timeout(startupTimeout)
    val result = controller ?? CreateGateway(Props(creator))
    httpGatewayPort.completeWith(result)
    result
  }


  def startContainers(): Seq[Future[Unit]] = {
    if (conf.Affi.Node.Containers.isDefined) {
      conf.Affi.Node.Containers().asScala.toList.map {
        case (group: String, value: CfgIntList) =>
          val partitions = value().asScala.map(_.toInt).toList
          startContainer(group, partitions)
      }
    } else List.empty
  }

  implicit val scheduler = system.scheduler

  def startContainer(group: String, partitions: List[Int]): Future[Unit] = {
    val serviceClass = conf.Affi.Keyspace(group).PartitionClass()
    implicit val timeout = Timeout(startupTimeout)
    controller ?? CreateContainer(group, partitions, Props(serviceClass.newInstance()))
  }

  def startContainer[T <: Partition](group: String, partitions: List[Int], partitionCreator: => T)
                                    (implicit tag: ClassTag[T]): Future[Unit] = {
    implicit val timeout = Timeout(startupTimeout)
    controller ?? CreateContainer(group, partitions, Props(partitionCreator))
  }


}
