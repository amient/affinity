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
import io.amient.affinity.core.cluster.Coordinator.CoordinatorConf
import io.amient.affinity.core.cluster.CoordinatorZk.CoordinatorZkConf
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.util.{ZkClients, ZkConf}
import org.I0Itec.zkclient.IZkChildListener
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._

object CoordinatorZk {

  object CoordinatorZkConf extends CoordinatorZkConf {
    override def apply(config: Config) = new CoordinatorZkConf()(config)
  }

  class CoordinatorZkConf extends CfgStruct[CoordinatorZkConf](classOf[CoordinatorConf]) {
    val ZooKeeper = struct("zookeeper", new ZkConf, true)
    val ZkRoot = string("zookeeper.root", "/affinity")
      .doc("znode under which coordination data between affinity nodes will be registered")
  }

}

class CoordinatorZk(system: ActorSystem, group: String, _conf: CoordinatorConf) extends Coordinator(system, group) {
  val conf = CoordinatorZkConf(_conf)
  val zkConf = conf.ZooKeeper()
  val zkRoot = conf.ZkRoot()
  val groupRoot = s"$zkRoot/${system.name}/$group/online"
  val peersRoot = s"$zkRoot/${system.name}/$group/peers"

  private val zk = ZkClients.get(zkConf)

  if (!zk.exists(groupRoot)) zk.createPersistent(groupRoot, true)

  updateChildren(zk.subscribeChildChanges(groupRoot, new IZkChildListener() {
    override def handleChildChange(parentPath: String, children: util.List[String]): Unit = {
      updateChildren(children)
    }
  }))

  override def register(actorPath: ActorPath): String = {
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
      val newState = newHandles.map(handle => (handle, zk.readData[String](handle))).toMap
      updateGroup(newState)
    }
  }

  override def registerPeer(akkaAddress: String, knownZid: Option[String]): String = {

    if (!zk.exists(peersRoot)) zk.createPersistent(peersRoot, true)
    val nodes = zk.getChildren(peersRoot).asScala.map(i => (i, zk.readData[String](s"$peersRoot/$i")))
    val zid: String = knownZid match {
      case Some(id) => nodes.find(_._1 == id) match {
        case Some((_, prevAkkaAddress)) if (prevAkkaAddress == akkaAddress) => id
        case Some(_) => zk.writeData(s"$peersRoot/$id", akkaAddress); id
        case None => throw new IllegalMonitorStateException(s"zid is invalid for group $group at $akkaAddress")
      }
      case None => nodes.find(_._2 == akkaAddress) match {
        case Some((id, _)) => id
        case None => zk.create(s"$peersRoot/", akkaAddress, CreateMode.PERSISTENT_SEQUENTIAL).substring(peersRoot.length+1)
      }
    }
    def update(zids: util.List[String]) = updatePeers(zids.asScala.toList)
    try zid finally update(zk.subscribeChildChanges(peersRoot, new IZkChildListener() {
      override def handleChildChange(parentPath: String, zids: util.List[String]): Unit = update(zids)
    }))
  }




}

