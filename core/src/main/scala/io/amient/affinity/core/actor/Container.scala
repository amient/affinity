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

package io.amient.affinity.core.actor

import java.io.{File, PrintWriter}
import java.nio.file.Files

import akka.actor.{Actor, ActorPath, ActorRef, Props}
import akka.event.Logging
import akka.util.Timeout
import io.amient.affinity
import io.amient.affinity.Conf
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Container._
import io.amient.affinity.core.cluster.Coordinator
import io.amient.affinity.core.cluster.Coordinator.MembershipUpdate
import io.amient.affinity.core.util.Reply

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

object Container {

  case class UpdatePeers(peers: List[String])

  case class AssignPartition(p: Int, props: Props) extends Reply[ActorRef]

  case class UnassignPartition(p: Int) extends Reply[Unit]

  case class PartitionOnline(partition: ActorRef)

  case class PartitionOffline(partition: ActorRef)

}

class Container(group: String) extends Actor {

  private val logger = Logging.getLogger(context.system, this)

  private val conf = Conf(context.system.settings.config)

  private val startupTimeout = conf.Affi.Node.StartupTimeoutMs().toLong milliseconds

  final private val akkaAddress = if (conf.Akka.Hostname() > "") {
    s"akka.tcp://${context.system.name}@${conf.Akka.Hostname()}:${conf.Akka.Port()}"
  } else {
    s"akka://${context.system.name}"
  }

  private val partitions = scala.collection.mutable.Map[ActorRef, String]()

  private val partitionIndex = scala.collection.mutable.Map[Int, ActorRef]()

  private val coordinator = Coordinator.create(context.system, group)

  private val masters = scala.collection.mutable.Set[ActorRef]()

  private var zid: Option[String] = None

  override def preStart(): Unit = {
    super.preStart()
    if (conf.Affi.Node.DataAutoAssign()) {
      if (!conf.Affi.Node.DataDir.isDefined) {
        logger.warning("node.data.auto.assign is enabled but node.data.dir is not defined - system's affinity to existing state will be minimal")
        zid = Some(coordinator.registerAndWatchPeers(akkaAddress, None, self))
      } else {
        val dir = conf.Affi.Node.DataDir()
        val zidFile = dir.resolve(s"$group.zid").toFile
        if (!zidFile.exists()) {
          zidFile.getParentFile.mkdirs()
          zidFile.createNewFile()
        } else {
          val source = scala.io.Source.fromFile(zidFile)
          zid = Some((try source.mkString finally source.close()))
        }
        zid = Some(coordinator.registerAndWatchPeers(akkaAddress, zid, self))
        new PrintWriter(zidFile) {
          write(zid.get.toString); close
        }
      }
    }
    //from this point on MembershipUpdate messages will be received by this Container when members are added/removed
    coordinator.watch(self)
  }

  override def postStop(): Unit = {
    if (!coordinator.isClosed) {
      coordinator.unwatch(self)
      partitions.filter(_._2 != null).foreach { case (ref, handle) =>
        logger.debug(s"Unregistering partition: handle=${handle}, path=${ref.path}")
        coordinator.unregister(handle)
      }
      coordinator.close()
    }
    super.postStop()
  }

  implicit val dispatcher = context.dispatcher
  implicit val scheduler = context.system.scheduler

  override def receive: Receive = {

    case UpdatePeers(peers: Seq[String]) if conf.Affi.Node.DataAutoAssign() && conf.Affi.Keyspace(group).isDefined =>
      peers.sorted.zipWithIndex.find(_._1 == zid.get).map(_._2) match {
        case None => throw new IllegalStateException(s"This peer is not registered: $akkaAddress")
        case Some(a) =>
          val ksConf: affinity.KeyspaceConf = conf.Affi.Keyspace(group)
          val serviceClass = ksConf.PartitionClass()
          val n = peers.size
          val np = ksConf.Partitions()
          val nr = np * ksConf.ReplicationFactor()
          val assigned = (0 until nr).filter(_ % n == a).map(_ % np).toSet
          val unassigned = (0 until np).toSet -- assigned
          assigned.foreach(partition => self ! AssignPartition(partition, Props(serviceClass)))
          unassigned.foreach(partition => self ! UnassignPartition(partition))
      }

    case request@AssignPartition(p, props) => request(sender) ! {
      partitionIndex.get(p) match {
        case Some(ref) => ref
        case None =>
          logger.debug(s"$akkaAddress: Assigning partition $p")
          val ref = context.actorOf(props, name = p.toString)
          partitionIndex += p -> ref
          ref
      }
    }

    case request@UnassignPartition(p) => request(sender) ! {
      if (partitionIndex.contains(p)) {
        logger.debug(s"$akkaAddress: Unassigning partition $p")
        partitionIndex.remove(p).foreach(context.stop)
      }
      if (conf.Affi.Node.DataDir.isDefined && conf.Affi.Node.DataAutoDelete()) {
        val dir = conf.Affi.Node.DataDir()
        if (Files.exists(dir)) {
          def deleteDirectory(f: File): Unit = if (f.exists) {
            if (f.isDirectory) f.listFiles.foreach(deleteDirectory)
            if (!f.delete) throw new RuntimeException(s"Failed to delete ${f.getAbsolutePath}")
          }
          Files.newDirectoryStream(dir, s"$group-*-$p").asScala.foreach { partDir =>
            logger.warning(s"$akkaAddress: Deleting unassigned partition data: $partDir")
            deleteDirectory(partDir.toFile)
          }
        }
      }
    }

    case PartitionOnline(ref) =>
      val partitionActorPath = ActorPath.fromString(s"${akkaAddress}${ref.path.toStringWithoutAddress}")
      val handle = coordinator.register(partitionActorPath)
      logger.debug(s"Partition online: handle=$handle, path=${partitionActorPath}")
      partitions += (ref -> handle)

    case PartitionOffline(ref) =>
      logger.debug(s"Partition offline: handle=${partitions.get(ref)}, path=${ref.path}")
      coordinator.unregister(partitions(ref))
      partitions -= ref

    case request: MembershipUpdate => request(sender) ! {
      //get cluster-wide master delta
      val (_add, _remove) = request.mastersDelta(masters)
      //filter out non-local changes and apply
      val remove = _remove.filter(_.path.address == self.path.address)
      masters --= remove
      val add = _add.filter(_.path.address == self.path.address)
      masters ++= add
      implicit val timeout = Timeout(startupTimeout)
      remove.foreach(_ ?! BecomeStandby())
      add.foreach(_ ?! BecomeMaster())
    }
  }
}
