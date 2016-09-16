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

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.core.ack._

import scala.concurrent.Await
import scala.concurrent.duration._

object Coordinator {

  final val CONFIG_COORDINATOR_CLASS = "affinity.cluster.coordinator.class"

  final case class AddMaster(group: String, ref: ActorRef)

  final case class RemoveMaster(group: String, ref: ActorRef)

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
    * @param watcher actor which will receive the messages
    */
  def watch(watcher: ActorRef, global: Boolean): Unit = {
    synchronized {
      watchers += watcher -> true
      handles.map(_._2.path.toStringWithoutAddress).toSet[String].foreach { relPath =>
        val replicas = handles.filter(_._2.path.toStringWithoutAddress == relPath)
        val masterActorRef = replicas.minBy(_._1)._2
        //failing this ack means that the watcher would have inconsistent view of the cluster
        if (global || masterActorRef.path.address.hasLocalScope) {
          ack(watcher, AddMaster(group, masterActorRef)) onFailure {
            case e: Throwable =>
              e.printStackTrace()
              system.terminate()
          }
        }
      }
    }
  }


  def unwatch(watcher: ActorRef): Unit = {
    synchronized {
      watchers -= watcher
    }
  }

  def unwatchAll() = synchronized {
    watchers.clear()
  }

  def close(): Unit

  final protected def updateMembers(newState: Map[String, String]) = {
    synchronized {
      newState.foreach { case (handle, actorPath) => if (!handles.keySet.contains(handle))
        addRouteeActor(handle, actorPath, force = false)
      }
      handles.keySet.filter(!newState.contains(_)).foreach { handle =>
        removeRoutee(handle)
      }
    }
  }

  private def notifyWatchers(message: Any, hasLocalScope: Boolean): Unit = {
    synchronized {
      watchers.foreach { case (watcher, global) =>
        if (global || hasLocalScope) {
          ack(watcher, message) onFailure {
            case e: Throwable =>
              e.printStackTrace()
              system.terminate()
          }
        }
      }
    }
  }

  private def removeRoutee(routeeHandle: String): Unit = {
    if (handles.contains(routeeHandle)) {
      handles.get(routeeHandle) match {
        case None =>
        case Some(master) =>
          val currentReplicas = handles.filter(_._2.path.toStringWithoutAddress == master.path.toStringWithoutAddress)
          val currentMaster = currentReplicas.minBy(_._1)
          handles.remove(routeeHandle)
          notifyWatchers(RemoveMaster(group, master), master.path.address.hasLocalScope)
          val replicas = handles.filter(_._2.path.toStringWithoutAddress == master.path.toStringWithoutAddress)
          if (replicas.size > 0) {
            val newMaster = replicas.minBy(_._1)
            if (currentMaster != newMaster) {
              notifyWatchers(AddMaster(group, newMaster._2), newMaster._2.path.address.hasLocalScope)
            }
          } else {
            //TODO last replica removed - no master available, what do we do ?
          }
      }
    }
  }

  private def addRouteeActor(routeeHandle: String,
                             routeePath: String,
                             force: Boolean = false): Unit = {
    val t = 6 seconds
    implicit val timeout = new Timeout(t)
    try {
      val routeeRef = Await.result(system.actorSelection(routeePath).resolveOne(), t)
      if (force || !handles.contains(routeeHandle)) {
        val relPath = routeeRef.path.toStringWithoutAddress
        val replicas = handles.filter(_._2.path.toStringWithoutAddress == relPath)
        handles.put(routeeHandle, routeeRef)
        if (replicas.size > 0) {
          val (currentMasterHandle, currentMasterActorRef) = replicas.minBy(_._1)
          if (routeeHandle < currentMasterHandle) {
            notifyWatchers(RemoveMaster(group, currentMasterActorRef), currentMasterActorRef.path.address.hasLocalScope)
            notifyWatchers(AddMaster(group, routeeRef), routeeRef.path.address.hasLocalScope)
          }
        } else {
          notifyWatchers(AddMaster(group, routeeRef), routeeRef.path.address.hasLocalScope)
        }
      }
    } catch {
      case e: Throwable => e.printStackTrace() //TODO handle this by recursive retry few times and than exit the system
    }

  }
}
