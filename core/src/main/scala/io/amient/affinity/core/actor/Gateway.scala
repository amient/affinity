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
import io.amient.affinity.core.actor.Controller.{CreateGateway, GracefulShutdown}
import io.amient.affinity.core.actor.Keyspace.{CheckKeyspaceStatus, KeyspaceStatus}
import io.amient.affinity.core.cluster.Coordinator
import io.amient.affinity.core.cluster.Coordinator.MasterUpdates
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.storage.{LogStorageConf, State}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Gateway {

  class GatewayConf extends CfgStruct[GatewayConf] {
    val Class = cls("class", classOf[Gateway], false).doc("Entry point class for all external requests, both http and stream inputs")
    val SuspendQueueMaxSize = integer("suspend.queue.max.size", 1000).doc("Size of the queue when the cluster enters suspended mode")
    val Http = struct("http", new GatewayHttp.HttpConf)
    val Stream = group("stream", classOf[LogStorageConf], false).doc("External input and output streams to which system is connected, if any")
  }

  final case class GatewayClusterStatus(suspended: Boolean)

}

trait Gateway extends ActorHandler {

  private val log = Logging.getLogger(context.system, this)

  import Gateway._

  private val conf = Conf(context.system.settings.config).Affi

  implicit def javaToScalaFuture[T](jf: java.util.concurrent.Future[T]): Future[T] = Future(jf.get)(ExecutionContext.Implicits.global)

  implicit def unitToVoidFuture(f: Future[Unit]): Future[Void] = f.map(null)(ExecutionContext.Implicits.global)

  import context.dispatcher

  private var started = false

  /**
    * internal map of all declared keyspaces (keyspace: String -> (Coordinator, KeyspaceActorRef, SuspendedFlag)
    */
  private val declaredKeyspaces = mutable.Map[String, (Coordinator, ActorRef, AtomicBoolean)]()
  private lazy val keyspaces = declaredKeyspaces.toMap

  /**
    * internal lisf of all declared globals and their final version referenced global state stores
    */
  private val declaredGlobals = mutable.Map[String, State[_, _]]()
  private lazy val globals = declaredGlobals.toMap

  /**
    * internal gateway suspension flag is set to true whenever any of the keyspaces is suspended
    */
  private var handlingSuspended = true

  final def global[K: ClassTag, V: ClassTag](globalStateStore: String): State[K, V] = {
    if (started) throw new IllegalStateException("Cannot declare state after the actor has started")
    declaredGlobals.get(globalStateStore) match {
      case Some(globalState) => globalState.asInstanceOf[State[K, V]]
      case None =>
        val bc = State.create[K, V](globalStateStore, 0, conf.Global(globalStateStore), 1, context.system)
        declaredGlobals += (globalStateStore -> bc)
        bc
    }
  }

  final def keyspace(group: String): ActorRef = {
    if (started) throw new IllegalStateException("Cannot declare keyspace after the actor has started")
    declaredKeyspaces.get(group) match {
      case Some((_, keyspaceActor, _)) => keyspaceActor
      case None =>
        if (!handlingSuspended) throw new IllegalStateException("All required affinity services must be declared in the constructor")
        val serviceConf = conf.Keyspace(group)
        if (!serviceConf.isDefined) throw new IllegalArgumentException(s"Keypsace $group is not defined")
        val ks = context.actorOf(Props(new Keyspace(serviceConf.config())), name = group)
        val coordinator = Coordinator.create(context.system, group)
        context.watch(ks)
        declaredKeyspaces += (group -> ((coordinator, ks, new AtomicBoolean(true))))
        ks
    }
  }

  final def connectKeyValueMediator(keyspace: ActorRef, stateStoreName: String, key: Any): Future[ActorRef] = {
    implicit val timeout = Timeout(1 second)
    keyspace ? CreateKeyValueMediator(stateStoreName, key) collect {
      case KeyValueMediatorCreated(keyValueMediator) => keyValueMediator
    }
  }

  final def describeKeyspaces: Map[String, Seq[String]] = {
    if (!started) throw new IllegalStateException("Cannot get keyspace reference before the actor has started")
    val t = 60 seconds
    implicit val timeout = Timeout(t)
    keyspaces.map {
      case (group, (_, actorRef, _)) =>
        val x = Await.result(actorRef ? GetRoutees map (_.asInstanceOf[Routees]), t)
        (group, x.routees.map(_.toString))
    }
  }

  abstract override def preStart(): Unit = {
    super.preStart()
    started = true

    // first bootstrap all global state stores
    globals.values.foreach(_.boot)
    // any thereafter switch them to passive mode for the remainder of the runtime
    globals.values.foreach(_.tail)

    evaluateSuspensionStatus() //each gateway starts in a suspended mode so in case there are no keyspaces this will resume it

    // finally - set up a watch for each referenced keyspace coordinator
    // coordinator will be sending 2 types of messages for each individual keyspace reference:
    // 1. MasterUpdates(..) sent whenever a master actor is added or removed to/from the routing tables
    // 2. KeyspaceStatus(..)
    keyspaces.values.foreach {
      case (coordinator, _, _) => coordinator.watch(self, clusterWide = true)
    }
  }

  abstract override def postStop(): Unit = try shutdown() finally super.postStop()

  abstract override def shutdown(): Unit = {
    globals.foreach {
      case (identifier, state) => try {
        state.close()
      } catch {
        case NonFatal(e) => log.error(e, s"Could not close cleanly global state: $identifier ")
      }
    }
    keyspaces.foreach {
      case (identifier, (coordinator, _, _)) => try {
        coordinator.unwatch(self)
        coordinator.close()
      } catch {
        case NonFatal(e) => log.warning(s"Could not close coordinators for keyspace: $identifier", e);
      }
    }
  }

  abstract override def manage = super.manage orElse {

    case request@GracefulShutdown() => request(sender) ! {
      shutdown()
      log.info("Gateway shutdown completed")
      context.stop(self)
    }

    case CreateGateway if !classOf[GatewayHttp].isAssignableFrom(this.getClass) =>
      context.parent ! Controller.GatewayCreated(-1)
      if (conf.Node.Gateway.Http.isDefined) {
        log.warning("affinity.gateway.http interface is configured but the node is trying " +
          s"to instantiate a non-http gateway ${this.getClass}. This may lead to uncertainity in the Controller.")
      }

    case request@MasterUpdates(group, add, remove) => request(sender) ! {
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
