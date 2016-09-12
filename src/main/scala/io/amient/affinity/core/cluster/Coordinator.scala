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
import java.util.Properties

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.util.Timeout
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Coordinator {

  final val CONFIG_COORDINATOR_CLASS = "coordinator.class"

  final case class AddMaster(group: String, ref: ActorRef)

  final case class RemoveMaster(group: String, ref: ActorRef)

  def fromProperties(system: ActorSystem, group: String, appConfig: Properties): Coordinator = {
    val className = appConfig.getProperty(CONFIG_COORDINATOR_CLASS, classOf[ZkCoordinator].getName)
    val cls = Class.forName(className).asSubclass(classOf[Coordinator])
    val constructor = cls.getConstructor(classOf[ActorSystem], classOf[String], classOf[Properties])
    constructor.newInstance(system, group, appConfig)
  }
}

/**
  * @param group coordinated group name
  */
abstract class Coordinator(val system: ActorSystem, val group: String) {

  import Coordinator._

  /**
    * internal data structure
    */
  private val handles = scala.collection.mutable.Map[String, ActorRef]()

  private val watchers = scala.collection.mutable.ListBuffer[ActorRef]()


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
    * watch changes in the coordinate group of routees. The implementation must
    * call addRouteeActor() and removeRouteeActor() methods when respective
    * changes occur.
    *
    * @param watcher actor which will receive the messages
    */
  def watchRoutees(watcher: ActorRef): Unit = {
    synchronized {
      watchers += watcher
      handles.map(_._2.path.toStringWithoutAddress).toSet[String].foreach { relPath =>
        val replicas = handles.filter(_._2.path.toStringWithoutAddress == relPath)
        val (masterHandle, masterActorRef) = replicas.minBy(_._1)
        watcher ! AddMaster(group, masterActorRef)
      }
    }
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

  private def notifyWatchers(arg: Any): Unit = synchronized {
    watchers.foreach(_ ! arg)
  }

  private def removeRoutee(routeeHandle: String): Unit = {
    if (handles.contains(routeeHandle)) {
      handles.get(routeeHandle) match {
        case None =>
        case Some(master) =>
          val currentReplicas = handles.filter(_._2.path.toStringWithoutAddress == master.path.toStringWithoutAddress)
          val currentMaster = currentReplicas.minBy(_._1)
          handles.remove(routeeHandle)
          notifyWatchers(RemoveMaster(group, master))
          val replicas = handles.filter(_._2.path.toStringWithoutAddress == master.path.toStringWithoutAddress)
          if (replicas.size > 0) {
            val newMaster = replicas.minBy(_._1)
            if (currentMaster != newMaster) {
              notifyWatchers(AddMaster(group, newMaster._2))
            }
          } else {
            //TODO last replica removed - no master available
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
            notifyWatchers(RemoveMaster(group, currentMasterActorRef))
            notifyWatchers(AddMaster(group, routeeRef))
          }
        } else {
          notifyWatchers(AddMaster(group, routeeRef))
        }
      }
    } catch {
      case e: Throwable => e.printStackTrace() //TODO handle this by recursive retry few times and than exit the system
    }

  }
}

object ZkCoordinator {
  final val CONFIG_ZOOKEEPER_CONNECT = "coordinator.zookeeper.connect"
  final val CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS = "coordinator.zookeeper.connect.timeout.ms"
  final val CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS = "coordinator.zookeeper.session.timeout.ms"
  final val CONFIG_ZOOKEEPER_ROOT = "coordinator.zookeeper.root"
}

class ZkCoordinator(system: ActorSystem, group: String, appConfig: Properties) extends Coordinator(system, group) {

  import ZkCoordinator._

  val zkConnect = appConfig.getProperty(CONFIG_ZOOKEEPER_CONNECT, "localhost:2181")
  val zkConnectTimeout = appConfig.getProperty(CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS, "30000").toInt
  val zkSessionTimeout = appConfig.getProperty(CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS, "6000").toInt
  val zkRoot = appConfig.getProperty(CONFIG_ZOOKEEPER_ROOT, "/akka")
  val groupRoot = s"$zkRoot/$group"

  private val zk = new ZkClient(
    zkConnect, zkSessionTimeout, zkConnectTimeout, new ZkSerializer {
      def serialize(o: Object): Array[Byte] = o.toString.getBytes

      override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
    })

  if (!zk.exists(zkRoot)) zk.createPersistent(zkRoot, true)

  override def register(actorPath: ActorPath): String = {
    if (!zk.exists(groupRoot)) zk.createPersistent(groupRoot, true)
    zk.create(s"$groupRoot/", actorPath.toString(), CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  override def unregister(handle: String) = zk.delete(handle)

  override def close(): Unit = zk.close()


  private def listAsIndexedSeq(list: util.List[String]) = list.asScala.toIndexedSeq

  zk.subscribeChildChanges(groupRoot, new IZkChildListener() {
    override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
      if (currentChilds != null) {
        val newHandles = listAsIndexedSeq(currentChilds).map(id => s"$parentPath/$id")
        val newState = newHandles.map(handle => (handle, zk.readData(handle).asInstanceOf[String]))
        updateMembers(newState.toMap)
      }
    }
  })

}
