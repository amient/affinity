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
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import io.amient.affinity.core.Murmur2Partitioner
import io.amient.affinity.core.actor.Controller.CreateGateway
import io.amient.affinity.core.config.{CfgList, CfgStruct}
import io.amient.affinity.core.http.HttpInterfaceConf
import io.amient.affinity.core.state.{KVStore, KVStoreGlobal}
import io.amient.affinity.core.storage.LogStorageConf
import io.amient.affinity.core.util.AffinityMetrics

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Gateway {

  class GatewayConf extends CfgStruct[GatewayConf] {
    val Class = cls("class", classOf[Gateway], false).doc("Entry point class for all external requests, both http and stream inputs")
    val MaxWebSocketQueueSize = integer("max.websocket.queue.size", 100).doc("number of messages that can be queued for delivery before blocking")
    val Listeners: CfgList[HttpInterfaceConf] = list("listeners", classOf[HttpInterfaceConf], false).doc("list of listener interface configurations")
    val RejectSuspendedHttpRequests = bool("suspended.reject", true, true).doc("controls how http requests are treated in suspended state: true - immediately rejected with 503 Service Unavailable; false - enqueued for reprocessing on resumption")
    val Stream = group("stream", classOf[LogStorageConf], false).doc("External input and output streams to which system is connected, if any")
  }

  final case class GatewayClusterStatus(suspended: Boolean)

}

trait Gateway extends ActorHandler {

  import Gateway._

  implicit def javaToScalaFuture[T](jf: java.util.concurrent.Future[T]): Future[T] = Future(jf.get)(ExecutionContext.Implicits.global)

  implicit def unitToVoidFuture(f: Future[Unit]): Future[Void] = f.map(null)(ExecutionContext.Implicits.global)

  private implicit val executor = scala.concurrent.ExecutionContext.Implicits.global

  private var started = false

  private var offlineGroups = List[String]()

  private var stopping = false

  /**
    * internal map of all declared keyspaces (keyspace: String -> (Coordinator, KeyspaceActorRef, SuspendedFlag)
    */
  private val declaredKeyspaces = mutable.Map[String, (ActorRef, AtomicBoolean)]()
  private lazy val keyspaces: Map[String, (ActorRef, AtomicBoolean)] = declaredKeyspaces.toMap

  private val declaredGlobals = mutable.Map[String, (KVStoreGlobal[_, _], AtomicBoolean)]()
  private lazy val globals: Map[String, (KVStoreGlobal[_, _], AtomicBoolean)] = declaredGlobals.toMap

  val metrics = AffinityMetrics.forActorSystem(context.system)

  def trace(groupName: String, result: Promise[_ <: Any]): Unit = metrics.process(groupName, result)

  def trace(groupName: String, result: Future[Any]): Unit = metrics.process(groupName, result)

  final def keyspace(identifier: String): ActorRef = {
    if (started) throw new IllegalStateException("Cannot declare keyspace after the actor has started")
    declaredKeyspaces.get(identifier) match {
      case Some((keyspaceActor, _)) => keyspaceActor
      case None =>
        if (started) throw new IllegalStateException("All required affinity services must be declared in the constructor")
        val serviceConf = conf.Affi.Keyspace(identifier)
        if (!serviceConf.isDefined()) throw new IllegalArgumentException(s"Keypsace $identifier is not defined")
        val partitioner = new Murmur2Partitioner
        val ks = context.actorOf(Props(new Group(identifier, serviceConf.NumPartitions(), partitioner)), name = identifier)
        declaredKeyspaces += (identifier -> ((ks, new AtomicBoolean(true))))
        ks
    }
  }

  final def global[K: ClassTag, V: ClassTag](globalName: String): KVStore[K, V] = {
    if (started) throw new IllegalStateException("Cannot declare state after the actor has started")
    declaredGlobals.get(globalName) match {
      case Some((globalStore, _)) => globalStore.asInstanceOf[KVStoreGlobal[K, V]]
      case None =>
        val bc = new KVStoreGlobal[K, V](globalName, conf.Affi.Global(globalName), context)
        declaredGlobals += (globalName -> ((bc, new AtomicBoolean(true))))
        bc
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
      case (group, (actorRef, _)) =>
        val x = Await.result(actorRef ? GetRoutees map (_.asInstanceOf[Routees]), t)
        (group, x.routees.map(_.toString))
    }
  }

  abstract override def preStart(): Unit = {
    super.preStart()
    //each gateway starts in a suspended mode so in case there are no keyspaces or globals the following will resume it
    evaluateSuspensionStatus()
    started = true
  }

  abstract override def postStop(): Unit = try {
    logger.debug("Closing global state stores")
    stopping = true
    if (offlineGroups.size > 0) {
      logger.warning(s"Offline groups: ${offlineGroups.mkString(", ")}; in ${self.path}")
    }
    globals.foreach { case (identifier, (store, _)) => try {
      store.close()
    } catch {
      case NonFatal(e) => logger.error(e, s"Could not close cleanly global state: $identifier ")
    }
    }
  } finally {
    super.postStop()
  }

  abstract override def manage = super.manage orElse {

    case CreateGateway if !classOf[GatewayHttp].isAssignableFrom(this.getClass) =>
      context.parent ! Controller.GatewayCreated(List())
      if (conf.Affi.Node.Gateway.Listeners.isDefined) {
        logger.warning("affinity.gateway has listeners configured but the node is trying " +
          s"to instantiate a non-http gateway ${this.getClass}. This may lead to uncertainity in the Controller.")
      }
    case msg@GroupStatus(group, suspended) if keyspaces.contains(group) =>
      if (keyspaces(group)._2.get != suspended) {
        keyspaces(group)._2.set(suspended)
        evaluateSuspensionStatus(Some(msg))
      }

    case msg@GroupStatus(group, suspended) if globals.contains(group) =>
      if (globals(group)._2.get != suspended) {
        globals(group)._2.set(suspended)
        evaluateSuspensionStatus(Some(msg))
      }

    case GroupStatus(group, _) => logger.warning(s"GroupStatus for unrecognized group type: $group")

  }

  private def evaluateSuspensionStatus(msg: Option[AnyRef] = None): Unit = {
    val shouldBeSuspended = keyspaces.exists(_._2._2.get) || globals.exists(_._2._2.get)
    if (shouldBeSuspended != isSuspended) {
      if (shouldBeSuspended) suspend else resume
      if (self.path.name == "gateway") {
        //if this is an actual external gateway (as opposed to gateway trait mixed into partition)
        //publish some messages for synchronizing test utitlities
        msg.foreach(context.system.eventStream.publish) // this one is for IntegrationTestBase
        context.system.eventStream.publish(GatewayClusterStatus(isSuspended)) //this one for SystemTestBase
      }
    }
    if (started && isSuspended && !stopping) {
      offlineGroups = (keyspaces.filter(_._2._2.get) ++ globals.filter(_._2._2.get)).map(_._1).toList
    }
  }

}
