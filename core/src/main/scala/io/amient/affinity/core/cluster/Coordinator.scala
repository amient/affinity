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

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.core.ack._

import scala.collection.Set
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

object Coordinator {

  final val CONFIG_COORDINATOR_CLASS = "affinity.cluster.coordinator.class"

  final case class MasterStatusUpdate(group: String, add: Set[ActorRef], remove: Set[ActorRef])

  def create(system: ActorSystem, group: String): Coordinator = {
    val config = system.settings.config
    val className = config.getString(CONFIG_COORDINATOR_CLASS)
    val cls = Class.forName(className).asSubclass(classOf[Coordinator])
    val constructor = cls.getConstructor(classOf[ActorSystem], classOf[String], classOf[Config])
    constructor.newInstance(system, group, config)
  }
}

/**
  * @param group coordinated group name
  */
abstract class Coordinator(val system: ActorSystem, val group: String) {

  import Coordinator._
  import system.dispatcher

  private val handles = scala.collection.mutable.Map[String, ActorRef]()

  private val closed = new AtomicBoolean(false)

  /**
    * wacthers - a list of all actors that will receive AddMaster and RemoveMaster messages
    * when there are changes in the cluster. The value is global flag - `true` means that the
    * watcher is interested for changes at the global/cluster level, `false` means that the
    * watcher is only intereseted in changes in the local system.
    */
  private val watchers = scala.collection.mutable.Map[ActorRef, Boolean]()

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
  def unregister(handle: String)

  /**
    * watch changes in the coordinate group of routees in the whole cluster.
    *
    * @param watcher actor which will receive the messages
    */
  def watch(watcher: ActorRef, global: Boolean): Unit = {
    synchronized {
      watchers += watcher -> true
      //failing this ack means that the watcher would have inconsistent view of the cluster
      val currentMasters = getCurrentMasters.filter(global || _.path.address.hasLocalScope)
      //TODO this ack can take very long if the accumulated state change log is large so need variable ack timeout
      ack(watcher, MasterStatusUpdate(group, currentMasters, Set())) onFailure {
        case e: Throwable => if (!closed.get) {
          e.printStackTrace()
          system.terminate()
        }
      }
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


  final protected def updateGroup(newState: Map[String, String]) = {
    val t = 6 seconds
    implicit val timeout = new Timeout(t)
    synchronized {
      val prevMasters: Set[ActorRef] = getCurrentMasters
      handles.clear()
      newState.foreach { case (handle, actorPath) =>
        try {
          handles.put(handle, Await.result(system.actorSelection(actorPath).resolveOne(), t))
        } catch {
          case NonFatal(e) => //TODO most likely the actor has gone and there will be another update right away but could be something else
        }
      }

      val currentMasters: Set[ActorRef] = getCurrentMasters

      val add = currentMasters.filter(!prevMasters.contains(_))
      val remove = prevMasters.filter(!currentMasters.contains(_))
      val fullUpdate = MasterStatusUpdate(group, add, remove)

      notifyWatchers(fullUpdate)
    }

  }

  private def getCurrentMasters: Set[ActorRef] = {
    handles.map(_._2.path.toStringWithoutAddress).toSet[String].map { relPath =>
      handles.filter(_._2.path.toStringWithoutAddress == relPath).minBy(_._1)._2
    }
  }


  private def notifyWatchers(fullUpdate: MasterStatusUpdate) = {
    def isLocal(ref: ActorRef) = ref.path.address.hasLocalScope

    val localUpdate = MasterStatusUpdate(group, fullUpdate.add.filter(isLocal), fullUpdate.remove.filter(isLocal))

    watchers.foreach { case (watcher, global) =>
      ack(watcher, if (global) fullUpdate else localUpdate) onFailure {
        case e: Throwable =>
          e.printStackTrace()
          system.terminate()
      }
    }
  }


}
