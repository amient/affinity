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

import java.util.Properties

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.event.Logging
import io.amient.affinity.core.cluster.Coordinator

object Node {
  final val CONFIG_AKKA_HOST = "gateway.akka.host"
  final val CONFIG_AKKA_PORT = "gateway.akka.port"

  case class ServiceOnline(partition: ActorRef)

  case class ServiceOffline(partition: ActorRef)

}

class Node(appConfig: Properties, coordinator: Coordinator, group: String) extends Actor {

  val log = Logging.getLogger(context.system, this)

  import Node._

  val akkaPort = appConfig.getProperty(CONFIG_AKKA_PORT, "2552").toInt
  val akkaAddress = appConfig.getProperty(CONFIG_AKKA_HOST, null) match {
    case null => s"akka://${context.system.name}"
    case host => s"akka.tcp://${context.system.name}@${host}:${akkaPort}"
  }

  private val services = scala.collection.mutable.Map[ActorRef, String]()

  override def postStop(): Unit = {
    services.foreach { case (ref, handle) =>
      log.info(s"Unregistering service: handle=${services(ref)}, path=${ref.path}")
      coordinator.unregister(services(ref))
    }
  }

  override def receive: Receive = {
    case ServiceOnline(ref) =>
      val partitionActorPath = ActorPath.fromString(s"${akkaAddress}${ref.path.toStringWithoutAddress}")
      val handle = coordinator.register(group, partitionActorPath)
      log.info(s"Service online: handle=$handle, path=${partitionActorPath}")
      services += (ref -> handle)

    case ServiceOffline(ref) =>
      log.info(s"Service offline: handle=${services(ref)}, path=${ref.path}")
      coordinator.unregister(services(ref))
      services -= ref
  }
}
