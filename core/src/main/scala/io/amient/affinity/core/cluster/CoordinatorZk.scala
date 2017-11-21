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

import akka.actor.{ActorPath, ActorSystem}
import com.typesafe.config.Config
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._

object CoordinatorZk {
  final val CONFIG_ZOOKEEPER_ROOT = "affinity.coordinator.zookeeper.root"
  final val CONFIG_ZOOKEEPER_CONNECT = "affinity.coordinator.zookeeper.connect"
  final val CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS = "affinity.coordinator.zookeeper.timeout.connect.ms"
  final val CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS = "affinity.coordinator.zookeeper.timeout.session.ms"
}

class CoordinatorZk(system: ActorSystem, group: String, config: Config) extends Coordinator(system, group) {

  import CoordinatorZk._

  val zkConnect = config.getString(CONFIG_ZOOKEEPER_CONNECT)
  val zkConnectTimeout = config.getInt(CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS)
  val zkSessionTimeout = config.getInt(CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS)
  val zkRoot = config.getString(CONFIG_ZOOKEEPER_ROOT)
  val groupRoot = s"$zkRoot/$group"

  private val zk = new ZkClient(zkConnect, zkSessionTimeout, zkConnectTimeout, new ZkSerializer {
    def serialize(o: Object): Array[Byte] = o.toString.getBytes
    override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
  })

  private var currentState = Map[String, String]()

  if (!zk.exists(zkRoot)) zk.createPersistent(groupRoot, true)

  val initialChildren = zk.subscribeChildChanges(groupRoot, new IZkChildListener() {
    override def handleChildChange(parentPath: String, children: util.List[String]): Unit = {
      updateChildren(children)
    }
  })

  updateChildren(initialChildren)

  override def register(actorPath: ActorPath): String = {
    if (!zk.exists(groupRoot)) zk.createPersistent(groupRoot, true)
    zk.create(s"$groupRoot/", actorPath.toString(), CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  override def unregister(handle: String) = zk.delete(handle)

  override def close(): Unit = {
    super.close()
    zk.close()
  }

  private def listAsIndexedSeq(list: util.List[String]) = list.asScala.toIndexedSeq

  private def updateChildren(children: util.List[String]) = {
    if (children != null) {
      val newHandles = listAsIndexedSeq(children).map(id => s"$groupRoot/$id")
      val newState = newHandles.map(handle => (handle, zk.readData(handle).asInstanceOf[String]))
      updateGroup(newState.toMap)
    }
  }

}

