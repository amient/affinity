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
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.routing.{ActorRefRoutee, AddRoutee, RemoveRoutee}
import akka.util.Timeout
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object Coordinator {

  final val CONFIG_COORDINATOR_CLASS = "coordinator.class"

  final case class AddService(ref: ActorRef)

  final case class RemoveService(ref: ActorRef)

  def fromProperties(appConfig: Properties): Coordinator = {
    val className = appConfig.getProperty(CONFIG_COORDINATOR_CLASS, classOf[ZkCoordinator].getName)
    val cls = Class.forName(className).asSubclass(classOf[Coordinator])
    val constructor = cls.getConstructor(classOf[Properties])
    constructor.newInstance(appConfig)
  }


}

trait Coordinator {

  import Coordinator._

  /**
    * @param group node type / group
    * @param actorPath of the actor that needs to managed as part of coordinated group
    * @return unique coordinator handle which points to the registered ActorPath
    */
  def register(group:String, actorPath: ActorPath): String

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
    * @param system  ActorSystem instance
    * @param watcher actor which will receive the messages
    */
  def watchRoutees(system: ActorSystem, group:String, watcher: ActorRef): Unit

  def close(): Unit

  /**
    * internal data structure
    */
  private val handles = new ConcurrentHashMap[String, ActorRef]()

  def getAllHandles: Set[String] = handles.keySet().asScala.toSet

  protected def removeRoutee(watcher: ActorRef, routeeHandle: String): Unit = {
    if (handles.containsKey(routeeHandle)) {
      val entry = handles.remove(routeeHandle)
      if (entry != null) {
        if (entry.path.toStringWithoutAddress.startsWith("/user/controller/region")) {
          watcher ! RemoveRoutee(ActorRefRoutee(entry))
        } else {
          watcher ! RemoveService(entry)
        }
      }
    }
  }

  protected def addRouteeActor(system: ActorSystem,
                               watcher: ActorRef,
                               routeeHandle: String,
                               routeePath: String,
                               force: Boolean = false): Unit = {
    import system.dispatcher
    implicit val timeout = new Timeout(24 hours)
    system.actorSelection(routeePath).resolveOne() onSuccess  {
      case partitionActorRef =>
        if (force || !handles.containsKey(routeeHandle)) {
          handles.put(routeeHandle, partitionActorRef)
          if (partitionActorRef.path.toStringWithoutAddress.startsWith("/user/controller/region")) {
            watcher ! AddRoutee(ActorRefRoutee(partitionActorRef))
          } else {
            watcher ! AddService(partitionActorRef)
          }
        }
    }
  }


}

object ZkCoordinator {
  final val CONFIG_ZOOKEEPER_CONNECT = "coordinator.zookeeper.connect"
  final val CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS = "coordinator.zookeeper.connect.timeout.ms"
  final val CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS = "coordinator.zookeeper.session.timeout.ms"
  final val CONFIG_ZOOKEEPER_ROOT = "coordinator.zookeeper.root"
}

class ZkCoordinator(appConfig: Properties) extends Coordinator {

  import ZkCoordinator._

  val zkConnect = appConfig.getProperty(CONFIG_ZOOKEEPER_CONNECT, "localhost:2181")
  val zkConnectTimeout = appConfig.getProperty(CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS, "30000").toInt
  val zkSessionTimeout = appConfig.getProperty(CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS, "6000").toInt
  val zkRoot = appConfig.getProperty(CONFIG_ZOOKEEPER_ROOT, "/akka")

  private val zk = new ZkClient(
    zkConnect, zkSessionTimeout, zkConnectTimeout, new ZkSerializer {
      def serialize(o: Object): Array[Byte] = o.toString.getBytes

      override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
    })

  if (!zk.exists(zkRoot)) zk.createPersistent(zkRoot, true)

  override def register(group:String, actorPath: ActorPath): String = {
    val groupRoot = s"$zkRoot/$group"
    if (!zk.exists(groupRoot)) zk.createPersistent(groupRoot, true)
    zk.create(s"$groupRoot/", actorPath.toString(), CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  override def unregister(handle: String) = zk.delete(handle)

  override def close(): Unit = zk.close()

  override def watchRoutees(system: ActorSystem, group: String, watcher: ActorRef): Unit = {

    def listAsIndexedSeq(list: util.List[String]) = list.asScala.toIndexedSeq

    val groupRoot = s"$zkRoot/$group"

    zk.subscribeChildChanges(groupRoot, new IZkChildListener() {
      override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
        if (currentChilds != null) {
          val newHandles = listAsIndexedSeq(currentChilds).map(id => s"$parentPath/$id")
          newHandles.foreach { handle =>
            addRouteeActor(system, watcher, handle, zk.readData(handle), force = false)
          }
          getAllHandles.filter(id => id.startsWith(parentPath) && !newHandles.contains(id)).foreach{ handle =>
            removeRoutee(watcher, handle)
          }
        }
      }
    })

    listAsIndexedSeq(zk.getChildren(groupRoot)).map(id => s"$groupRoot/$id").foreach{ handle =>
      addRouteeActor(system, watcher, handle, zk.readData(handle), force = true)
    }
  }

}
