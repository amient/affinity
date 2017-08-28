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

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.event.Logging
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Container._
import io.amient.affinity.core.actor.Controller.{ContainerOnline, GracefulShutdown}
import io.amient.affinity.core.actor.Partition.{BecomeMaster, BecomeStandby}
import io.amient.affinity.core.cluster.Coordinator.MasterStatusUpdate
import io.amient.affinity.core.cluster.{Coordinator, Node}

import scala.concurrent.duration._
import scala.language.postfixOps

object Container {

  case class ServiceOnline(partition: ActorRef)

  case class ServiceOffline(partition: ActorRef)

}

class Container(group: String) extends Actor {

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

  final private val akkaAddress = if (config.hasPath(Node.CONFIG_AKKA_HOST) && config.getString(Node.CONFIG_AKKA_HOST) > "") {
    s"akka.tcp://${context.system.name}@${config.getString(Node.CONFIG_AKKA_HOST)}:${config.getInt(Node.CONFIG_AKKA_PORT)}"
  } else {
    s"akka://${context.system.name}"
  }

  private val services = scala.collection.mutable.Map[ActorRef, String]()

  /**
    * This watch will result in localised MasterStatusUpdate messages to be send from the Cooridinator to this Container
    */
  private val coordinator = Coordinator.create(context.system, group)
  coordinator.watch(self, global = false)

  override def preStart(): Unit = {
    log.info(s"Starting container `$group` with ${context.children.size} services")
    super.preStart()
    context.children.foreach(services += _ -> null)
  }

  override def postStop(): Unit = {
    if (!coordinator.isClosed) {
      coordinator.unwatch(self)
      services.filter(_._2 != null).foreach { case (ref, handle) =>
        log.debug(s"Unregistering service: handle=${handle}, path=${ref.path}")
        coordinator.unregister(handle)
      }
    }
    super.postStop()
  }

  implicit val dispatcher = context.dispatcher
  implicit val scheduler = context.system.scheduler

  override def receive: Receive = {

    case ServiceOnline(ref) =>
      val partitionActorPath = ActorPath.fromString(s"${akkaAddress}${ref.path.toStringWithoutAddress}")
      val handle = coordinator.register(partitionActorPath)
      log.debug(s"Service online: handle=$handle, path=${partitionActorPath}")
      services += (ref -> handle)
      if (services.values.forall(_ != null)) {
        context.parent ! ContainerOnline(group)
      }

    case ServiceOffline(ref) =>
      log.debug(s"Service offline: handle=${services(ref)}, path=${ref.path}")
      coordinator.unregister(services(ref))
      services -= ref

    case request @ MasterStatusUpdate(_, add, remove) => sender.reply(request) {
      implicit val timeout = Timeout(5 seconds)
      remove.toList.map(ref => ref ack BecomeStandby())
      add.toList.map(ref => ref ack BecomeMaster())
    }

    case request@GracefulShutdown() => sender.reply(request) {
      context.stop(self)
    }

  }
}
