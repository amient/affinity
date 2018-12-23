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

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.event.Logging
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.Conf
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Container.UpdatePeers
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.util.{ByteUtils, Reply}

import scala.collection.JavaConverters._
import scala.collection.Set
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object Coordinator {

  object CoordinatorConf extends CoordinatorConf {
    override def apply(config: Config): CoordinatorConf = new CoordinatorConf().apply(config)
  }

  class CoordinatorConf extends CfgStruct[CoordinatorConf] {
    val Class = cls("class", classOf[Coordinator], classOf[CoordinatorZk])
      .doc("implementation of coordinator must extend cluster.Coordinator")

    override protected def specializations(): util.Set[String] = Set("zookeeper", "embedded").asJava
  }

  final case class MembershipUpdate(members: Map[String, ActorRef]) extends Reply[Unit] {

    /**
      * a method which is deterministic across multiple nodes that resolves which one of the replicas is the master for
      * each partition applying a murmur2 hash to the replica's unique handle and selecting the one with the largest hash
      * @return a set of all masters for each partition within the group this coordinator is managing
      */
    def masters: Set[ActorRef] = {
      members.map(_._2.path.elements.takeRight(2).mkString("/")).toSet[String].map { relPath =>
        members.filter(_._2.path.elements.takeRight(2).mkString("/") == relPath)
          .maxBy { case (handle, _) => ByteUtils.murmur2(handle.getBytes) }._2
      }
    }

    /**
      * mastersDelta splits the current members of this update message into additions and removals
      * with respect to the provided known state
      * @param knownMasters a set of actors to which if the result is applied will be the same as the members of this update
      * @return (list of additions, list of removals)
      */
    def mastersDelta(knownMasters: Set[ActorRef]): (List[ActorRef], List[ActorRef]) = {
      val newMasters = masters
      val add = newMasters.toList.filter(!knownMasters.contains(_))
      val remove = knownMasters.toList.filter(!newMasters.contains(_))
      (add, remove)
    }

  }

  def create(system: ActorSystem, group: String): Coordinator = {
    val config = system.settings.config
    val conf: CoordinatorConf = Conf(config).Affi.Coordinator
    val constructor = conf.Class().getConstructor(classOf[ActorSystem], classOf[String], classOf[CoordinatorConf])
    constructor.newInstance(system, group, conf)
  }
}

/**
  * @param group coordinated group name
  */
abstract class Coordinator(val system: ActorSystem, val group: String) {

  import Coordinator._
  import system.dispatcher

  implicit val scheduler = system.scheduler

  private val logger = Logging.getLogger(system, this)

  private val handles = scala.collection.mutable.Map[String, ActorRef]()

  def members: Map[String, String] = handles.toMap.mapValues(_.path.toString)

  protected val closed = new AtomicBoolean(false)

  /**
    * wacthers - a list of all actors that will receive MembershipUpdate messages when members are added or removed
    * to the group
    */
  protected val watchers = scala.collection.mutable.ListBuffer[ActorRef]()

  @volatile private var peerWatcher: ActorRef = null

  /**
    * @param actorPath of the actor that needs to managed as part of coordinated group
    * @return unique coordinator handle which points to the registered ActorPath
    */
  def register(actorPath: ActorPath): String

  /**
    * unregister previously registered ActorPath
    *
    * @param handle handler returned from the prior register() method call
    * @return
    */
  def unregister(handle: String): Unit

  /**
    * watch changes in the coordinate group of routees in the whole cluster.
    *
    * @param watcher     actor which will receive the messages
    */
  def watch(watcher: ActorRef): Unit = {
    synchronized {
      watchers += watcher
      updateWatcher(watcher)
    }
  }

  def unwatch(watcher: ActorRef): Unit = {
    synchronized {
      watchers -= watcher
    }
  }

  def close(): Unit = {
    closed.set(true)
    synchronized {
      watchers.clear()
      handles.clear()
    }
  }

  def isClosed = closed.get

  final protected def updateGroup(newState: Map[String, String]): Unit = if (!closed.get) {
    val attempts = Future.sequence(newState.map { case (handle, actorPath) =>
      handles.get(handle) match {
        case Some(knownActor) if knownActor.path == actorPath => Future.successful(Success((handle, knownActor)))
        case _ =>
          val selection = system.actorSelection(actorPath)
          implicit val timeout = new Timeout(30 seconds)
          selection.resolveOne() map (a => Success((handle, a))) recover {
            case NonFatal(e) =>
              logger.warning(s"$handle: ${e.getMessage}")
              Failure(e)
          }
      }
    })

    val actorRefs: Future[Iterable[(String, ActorRef)]] = attempts.map(_.collect {
      case Success((handle, actor)) => (handle, actor)
    })

    val newMembers = Await.result(actorRefs, 1 minute)

    synchronized {
      handles.clear()
      handles ++= newMembers
      updateWatchers()
    }

  }

  private def updateWatchers(): Unit = if (!closed.get) {
    val update = MembershipUpdate(handles.toMap)
    watchers.foreach(updateWatcher(_, Some(update)))
  }

  private def updateWatcher(watcher: ActorRef, _update: Option[MembershipUpdate] = None): Unit = if (!closed.get) {
    val update = _update.getOrElse(MembershipUpdate(handles.toMap))
    implicit val timeout = Timeout(30 seconds)
    try {
      (watcher ?! update).failed foreach {
        case e: Throwable => if (!closed.get) {
          logger.error(e, s"Could not notify watcher: $watcher")
        }
      }
    } catch {
      case e: Throwable => if (!closed.get) {
        logger.error(e, s"Could not notify watcher: $watcher")
      }
    }
  }

  final def registerAndWatchPeers(akkaAddress: String, zid: Option[String], watcher: ActorRef): String = {
    peerWatcher = watcher
    registerPeer(akkaAddress, zid)
  }

  final  protected def updatePeers(peers: List[String]) : Unit = peerWatcher ! UpdatePeers(peers)

  protected def registerPeer( akkaAddress: String, knownZid: Option[String]): String




}
