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
import io.amient.affinity.core.ack._
import io.amient.affinity.core.actor.Container._
import io.amient.affinity.core.actor.Controller.ContainerOnline
import io.amient.affinity.core.actor.Service.{BecomeMaster, BecomeStandby}
import io.amient.affinity.core.cluster.Coordinator.MasterStatusUpdate
import io.amient.affinity.core.cluster.{Coordinator, Node}
import scala.concurrent.duration._

import scala.concurrent.{Await, Future}

object Container {

  case class ServiceOnline(partition: ActorRef)

  case class ServiceOffline(partition: ActorRef)

}

class Container(coordinator: Coordinator, group: String) extends Actor {

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

  private val akkaAddress = if (config.hasPath(Node.CONFIG_AKKA_HOST)) {
    s"akka.tcp://${context.system.name}@${config.getString(Node.CONFIG_AKKA_HOST)}:${config.getInt(Node.CONFIG_AKKA_PORT)}"
  } else {
    s"akka://${context.system.name}"
  }

  private val services = scala.collection.mutable.Map[ActorRef, String]()

  /**
    * This watch will result in localised MasterStatusUpdate messages to be send from the Cooridinator to this Container
    */
  coordinator.watch(self, global = false)

  override def preStart(): Unit = {
    log.info(s"Starting container `$group` with ${context.children.size} services")
    super.preStart()
    context.children.foreach(services += _ -> null)
  }

  override def postStop(): Unit = {
    coordinator.unwatch(self)
    services.foreach { case (ref, handle) =>
      log.info(s"Unregistering service: handle=${services(ref)}, path=${ref.path}")
      coordinator.unregister(services(ref))
    }
    super.postStop()
  }

  import context.dispatcher

  override def receive: Receive = {
    case ServiceOnline(ref) =>
      val partitionActorPath = ActorPath.fromString(s"${akkaAddress}${ref.path.toStringWithoutAddress}")
      val handle = coordinator.register(partitionActorPath)
      log.info(s"Service online: handle=$handle, path=${partitionActorPath}")
      services += (ref -> handle)
      if (services.values.forall(_ != null)) {
        context.parent ! ContainerOnline(group)
      }

    case ServiceOffline(ref) =>
      log.info(s"Service offline: handle=${services(ref)}, path=${ref.path}")
      coordinator.unregister(services(ref))
      services -= ref

    case MasterStatusUpdate(_, add, remove) => replyWith(sender) {
      //TODO #12 global config bootstrap timeout
      val t = 30 seconds
      implicit val timeout = Timeout(t)
      val removals = remove.toList.map(ref => ack(ref, BecomeStandby()))
      val additions = add.toList.map(ref => ack(ref, BecomeMaster()))
      Future.sequence(removals ++ additions)
    }
  }
}
