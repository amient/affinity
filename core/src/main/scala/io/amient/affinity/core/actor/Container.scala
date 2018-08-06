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

import akka.actor.{Actor, ActorPath, ActorRef, Props}
import akka.event.Logging
import akka.util.Timeout
import io.amient.affinity.Conf
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Container._
import io.amient.affinity.core.cluster.Coordinator
import io.amient.affinity.core.cluster.Coordinator.MembershipUpdate
import io.amient.affinity.core.util.Reply

import scala.concurrent.duration._
import scala.language.postfixOps

object Container {

  case class AddPartition(p: Int, props: Props) extends Reply[ActorRef]

  case class DropPartition(p: Int) extends Reply[Unit]

  case class PartitionOnline(partition: ActorRef)

  case class PartitionOffline(partition: ActorRef)

}

class Container(group: String) extends Actor {

  private val log = Logging.getLogger(context.system, this)

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

  override def preStart(): Unit = {
    super.preStart()
    //from this point on MembershipUpdate messages will be received by this Container when members are added/removed
    coordinator.watch(self)
  }

  override def postStop(): Unit = {
    if (!coordinator.isClosed) {
      coordinator.unwatch(self)
      partitions.filter(_._2 != null).foreach { case (ref, handle) =>
        log.debug(s"Unregistering partition: handle=${handle}, path=${ref.path}")
        coordinator.unregister(handle)
      }
      coordinator.close()
    }
    super.postStop()
  }

  implicit val dispatcher = context.dispatcher
  implicit val scheduler = context.system.scheduler

  override def receive: Receive = {

    case request@AddPartition(p, props) => request(sender) ! {
      val ref = context.actorOf(props, name = p.toString)
      partitionIndex += p -> ref
      ref
    }

    case request@DropPartition(p) => request(sender) ! partitionIndex.remove(p).foreach(context.stop)

    case PartitionOnline(ref) =>
      val partitionActorPath = ActorPath.fromString(s"${akkaAddress}${ref.path.toStringWithoutAddress}")
      val handle = coordinator.register(partitionActorPath)
      log.debug(s"Partition online: handle=$handle, path=${partitionActorPath}")
      partitions += (ref -> handle)

    case PartitionOffline(ref) =>
      log.debug(s"Partition offline: handle=${partitions.get(ref)}, path=${ref.path}")
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
