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
import io.amient.affinity.Conf
import io.amient.affinity.core.cluster.Coordinator.CoorinatorConf
import io.amient.affinity.core.cluster.CoordinatorZk.CoordinatorZkConf
import io.amient.affinity.core.config.{CfgString, CfgStruct}
import io.amient.affinity.core.util.{ZkClients, ZkConf}
import org.I0Itec.zkclient.IZkChildListener
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._

object CoordinatorZk {

  object CoordinatorZkConf extends CoordinatorZkConf {
    override def apply(config: Config) = new CoordinatorZkConf()(config)
  }

  class CoordinatorZkConf extends CfgStruct[CoordinatorZkConf](classOf[CoorinatorConf]) {
    val ZooKeeper = struct("zookeeper", new ZkConf)
    val ZkRoot = string("zookeeper.root", "/affinity")
  }
}

class CoordinatorZk(system: ActorSystem, group: String, config: Config) extends Coordinator(system, group) {

  val conf = CoordinatorZkConf(Conf(system.settings.config).Affi.Coordinator)
  val zkConf = conf.ZooKeeper()
  val zkRoot = conf.ZkRoot()
  val groupRoot = s"$zkRoot/$group"

  private val zk = ZkClients.get(zkConf)

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

  override def close(): Unit = if (!closed.get) {
    super.close()
    ZkClients.close(zk);
  }

  private def listAsIndexedSeq(list: util.List[String]) = list.asScala.toIndexedSeq

  private def updateChildren(children: util.List[String]): Unit = {
    if (children != null) {
      val newHandles = listAsIndexedSeq(children).map(id => s"$groupRoot/$id")
      val newState = newHandles.map(handle => (handle, zk.readData(handle)))
      updateGroup(newState.toMap)
    }
  }

}

