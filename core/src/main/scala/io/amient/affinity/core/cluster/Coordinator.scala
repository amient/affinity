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
import io.amient.affinity.core.ack
import io.amient.affinity.core.util.Reply

import scala.collection.Set
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.language.postfixOps

object Coordinator {

  final val CONFIG_COORDINATOR_CLASS = "affinity.cluster.coordinator.class"

  final case class MasterStatusUpdate(group: String, add: Set[ActorRef], remove: Set[ActorRef]) extends Reply[Unit] {
    def localTo(actor: ActorRef): MasterStatusUpdate = {
      MasterStatusUpdate(
        group,
        add.filter(_.path.address == actor.path.address),
        remove.filter(_.path.address == actor.path.address)
      )
    }
  }

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

  implicit val scheduler = system.scheduler

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
    * @param global  if true, the watcher will be notified of master status changes in the entire cluster
    *                if false, the watcher will be notified of master status changes local to that watcher
    */
  def watch(watcher: ActorRef, global: Boolean): Unit = {
    synchronized {
      watchers += watcher -> global
      //failing this ack means that the watcher would have inconsistent view of the cluster
      val currentMasters = getCurrentMasters.filter(global || _.path.address.hasLocalScope)
      val update = MasterStatusUpdate(group, currentMasters, Set())
      //TODO  #12 global config bootstrap timeout
      implicit val timeout = Timeout(30 seconds)
      watcher ack (if (global) update else update.localTo(watcher)) onFailure {
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
          case NonFatal(e) =>
            //most likely the actor has gone and there will be another update right away but could be something else
            if (!closed.get) e.printStackTrace()
        }
      }

      val currentMasters: Set[ActorRef] = getCurrentMasters

      val add = currentMasters.filter(!prevMasters.contains(_))
      val remove = prevMasters.filter(!currentMasters.contains(_))
      val update = MasterStatusUpdate(group, add, remove)

      notifyWatchers(update)
    }

  }

  private def getCurrentMasters: Set[ActorRef] = {
    handles.map(_._2.path.toStringWithoutAddress).toSet[String].map { relPath =>
      handles.filter(_._2.path.toStringWithoutAddress == relPath).minBy(_._1)._2
    }
  }


  private def notifyWatchers(fullUpdate: MasterStatusUpdate) = {

    watchers.foreach { case (watcher, global) =>
      //TODO  #12 global config bootstrap timeout
      implicit val timeout = Timeout(30 seconds)
      try {
        watcher ack (if (global) fullUpdate else fullUpdate.localTo(watcher)) onFailure {
          case e: Throwable => if (!closed.get) {
            e.printStackTrace()
            system.terminate()
          }
        }
      } catch {
        case e: Throwable => if (!closed.get) {
          e.printStackTrace()
          system.terminate()
        }
      }
    }
  }


}
