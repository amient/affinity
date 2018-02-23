/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.core.actor

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import io.amient.affinity.Conf
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller.{CreateGateway, GracefulShutdown}
import io.amient.affinity.core.actor.Keyspace.{CheckKeyspaceStatus, KeyspaceStatus}
import io.amient.affinity.core.actor.Partition.{CreateKeyValueMediator, KeyValueMediatorCreated}
import io.amient.affinity.core.cluster.Coordinator
import io.amient.affinity.core.cluster.Coordinator.MasterUpdates
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import io.amient.affinity.core.storage.State

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Gateway {

  class GatewayConf extends CfgStruct[GatewayConf] {
    val Class = cls("class", classOf[Gateway], false)
    val SuspendQueueMaxSize = integer("suspend.queue.max.size", 1000)
    val Http = struct("http", new GatewayHttp.HttpConf)
    val Streams = group("stream", classOf[InputStreamConf], false)
  }

  final case class GatewayClusterStatus(suspended: Boolean)

  class InputStreamConf extends CfgStruct[InputStreamConf](Cfg.Options.IGNORE_UNKNOWN)

}

trait Gateway extends ActorHandler with ActorState {

  private val log = Logging.getLogger(context.system, this)

  import Gateway._

  private val conf = Conf(context.system.settings.config).Affi

  private implicit val scheduler = context.system.scheduler

  import context.dispatcher

  def onClusterStatus(suspended: Boolean): Unit = ()

  def global[K: ClassTag, V: ClassTag](globalStateStore: String): State[K, V] = {
    globals.get(globalStateStore) match {
      case Some(globalState) => globalState.asInstanceOf[State[K, V]]
      case None =>
        val bc = state[K, V](globalStateStore, conf.Global(globalStateStore))
        globals += (globalStateStore -> bc)
        bc
    }
  }

  def keyspace(group: String): ActorRef = {
    keyspaces.get(group) match {
      case Some((_, keyspaceActor, _)) => keyspaceActor
      case None =>
        if (!handlingSuspended) throw new IllegalStateException("All required affinity services must be declared in the constructor")
        val serviceConf = conf.Keyspace(group)
        if (!serviceConf.isDefined) throw new IllegalArgumentException(s"Keypsace $group is not defined")
        val ks = context.actorOf(Props(new Keyspace(serviceConf.config())), name = group)
        val coordinator = Coordinator.create(context.system, group)
        context.watch(ks)
        keyspaces += (group -> (coordinator, ks, new AtomicBoolean(true)))
        ks
    }
  }

  def connectKeyValueMediator(keyspace: ActorRef, stateStoreName: String, key: Any): Future[ActorRef] = {
    implicit val timeout = Timeout(1 second)
    keyspace ? CreateKeyValueMediator(stateStoreName, key) collect {
      case KeyValueMediatorCreated(keyValueMediator) => keyValueMediator
    }
  }

  def describeKeyspaces: Map[String, Seq[String]] = {
    val t = 60 seconds
    implicit val timeout = Timeout(t)
    keyspaces.toMap.map {
      case (group, (_, actorRef, _)) =>
        val x = Await.result(actorRef ? GetRoutees map (_.asInstanceOf[Routees]), t)
        (group, x.routees.map(_.toString))
    }
  }

  /**
    * internal map of all referenced keyspaces (keyspace: String -> (Coordinator, KeyspaceActorRef, SuspendedFlag)
    */
  private val keyspaces = mutable.Map[String, (Coordinator, ActorRef, AtomicBoolean)]()

  /**
    * internal lisf of all referenced global state stores
    */
  private val globals = mutable.Map[String, State[_, _]]()

  /**
    * internal gateway suspension flag is set to true whenever any of the keyspaces is suspended
    */
  private var handlingSuspended = true

  abstract override def preStart(): Unit = {
    super.preStart()
    activeState() // first bootstrap all global state stores
    passiveState() // any thereafter switch them to passive mode for the remainder of the runtime
    evaluateSuspensionStatus() //each gateway starts in a suspended mode so in case there are no keyspaces this will resume it

    // finally - set up a watch for each referenced keyspace coordinator
    // coordinator will be sending 2 types of messages for each individual keyspace reference:
    // 1. MasterUpdates(..) sent whenever a master actor is added or removed to/from the routing tables
    // 1. KeyspaceStatus(..)
    keyspaces.foreach {
      case (_, (coordinator, _, _)) => coordinator.watch(self, clusterWide = true)
    }
  }

  abstract override def postStop(): Unit = {
    try super.postStop() finally {
      globals.foreach {
        case (identifier, state) => try {
          state.close()
        } catch {
          case NonFatal(e) => log.error(e, s"Could not close cleanly global state: $identifier ")
        }
      }
      keyspaces.values.foreach {
        case (coordinator, _, _) => try {
          coordinator.unwatch(self)
          coordinator.close()
        } catch {
          case NonFatal(e) => log.warning("Could not close one of the gateway coordinators", e);
        }
      }
    }
  }

  abstract override def manage = super.manage orElse {

    case request@GracefulShutdown() => sender.reply(request) {
      context.stop(self)
    }

    case CreateGateway if !classOf[GatewayHttp].isAssignableFrom(this.getClass) =>
      context.parent ! Controller.GatewayCreated(-1)
      if (conf.Node.Gateway.Http.isDefined) {
        log.warning("affinity.gateway.http interface is configured but the node is trying " +
          s"to instantiate a non-http gateway ${this.getClass}. This may lead to uncertainity in the Controller.")
      }

    case msg@MasterUpdates(group, add, remove) => sender.reply(msg) {
      val service: ActorRef = keyspaces(group)._2
      remove.foreach(ref => service ! RemoveRoutee(ActorRefRoutee(ref)))
      add.foreach(ref => service ! AddRoutee(ActorRefRoutee(ref)))
      service ! CheckKeyspaceStatus(group)
    }

    case msg@KeyspaceStatus(_keyspace, suspended) =>
      val (_, _, keyspaceCurrentlySuspended) = keyspaces(_keyspace)
      if (keyspaceCurrentlySuspended.get != suspended) {
        keyspaces(_keyspace)._3.set(suspended)
        evaluateSuspensionStatus(Some(msg))
      }
  }

  private def evaluateSuspensionStatus(msg: Option[AnyRef] = None): Unit = {
    val gatewayShouldBeSuspended = keyspaces.exists(_._2._3.get)
    if (gatewayShouldBeSuspended != handlingSuspended) {
      handlingSuspended = gatewayShouldBeSuspended
      onClusterStatus(gatewayShouldBeSuspended)

      //if this is an actual external gateway (as opposed to gateway trait mixec into say partition)
      if (self.path.name == "gateway") {
        //publish some messages for synchronizing test utitlities
        msg.foreach(context.system.eventStream.publish) // this one is for IntegrationTestBase
        context.system.eventStream.publish(GatewayClusterStatus(gatewayShouldBeSuspended)) //this one for SystemTestBase
      }
    }
  }

}
