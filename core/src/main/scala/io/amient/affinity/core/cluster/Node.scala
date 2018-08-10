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


import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}

import akka.actor.{Actor, Props}
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller._
import io.amient.affinity.core.actor.Gateway.{GatewayClusterStatus, GatewayConf}
import io.amient.affinity.core.actor._
import io.amient.affinity.core.config._
import io.amient.affinity.{AffinityActorSystem, Conf}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag

object Node {

  class NodeConf extends CfgStruct[NodeConf] {
    val AdvertisedAddress = string("advertised.address", false).doc("<host>:<port> use this to advertise this node's address if it is running behind a proxy or inside a container")
    val Containers: CfgGroup[CfgIntList] = group("container", classOf[CfgIntList], false)
      .doc("Array of partitions assigned to this node, <ID> represents the Keyspace, e.g. assigning first four partitions of MyKeySpace: affinity.node.container.MyKeySpace = [0,1,2,3] ")
    val Gateway: GatewayConf = struct("gateway", new GatewayConf, false)
    val SuspendQueueMaxSize = integer("suspend.queue.max.size", 1000).doc("Size of the queue when the cluster enters suspended mode")
    val StartupTimeoutMs = longint("startup.timeout.ms", Integer.MAX_VALUE).doc("Maximum time a node can take to startup - this number must account for any potential state bootstrap")
    val ShutdownTimeoutMs = longint("shutdown.timeout.ms", 30000).doc("Maximum time a node can take to shutdown gracefully")
    val DataDir = filepath("data.dir", false).doc("Location under which any local state or registers will be kept")
    val DataAutoAssign = bool("data.auto.assign", true, false).doc("Determines whether this node auto-balances data its containers; if set tot false the fixed list of container partitions will be used")
    val DataAutoDelete = bool("data.auto.delete", true, false).doc("If set to true, any unassigned partitions will be deleted from the local storage")
  }

}

class Node(config: Config) {

  val conf = Conf(config)
  
  val startupTimeout = conf.Affi.Node.StartupTimeoutMs().toLong milliseconds
  val shutdownTimeout = conf.Affi.Node.ShutdownTimeoutMs().toLong milliseconds

  implicit val system = AffinityActorSystem.create(config)

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
    if (clusterReady.getCount > 0) {
      throw new TimeoutException(s"Cluster did not become ready in ${startupTimeout.toMillis} ms")
    }
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

    if (conf.Affi.Node.Gateway.Class.isDefined) {
      Await.result(start(conf.Affi.Node.Gateway.Class().newInstance()), startupTimeout)
    } else {
      controller ! StartRebalance()
      httpGatewayPort.success(List())
    }
  } catch {
    case e: Throwable =>
      try {
        Await.result(system.terminate(), shutdownTimeout)
      } finally {
        throw e
      }
  }

  /**
    * @param creator
    * @param tag
    * @tparam T
    * @return a future with list of ports indexed by the interface id 0-n
    */
  def start[T <: Gateway](creator: => T)(implicit tag: ClassTag[T]): Future[List[Int]] = {
    controller ! StartRebalance()
    implicit val timeout = Timeout(startupTimeout)
    val result = controller ?? CreateGateway(Props(creator))
    httpGatewayPort.completeWith(result)
    result
  }

}
