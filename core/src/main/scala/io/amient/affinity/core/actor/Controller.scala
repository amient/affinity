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

import akka.AkkaException
import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume}
import akka.actor.{Actor, ActorRef, InvalidActorNameException, OneForOneStrategy, Props, Terminated}
import akka.event.Logging
import akka.util.Timeout
import io.amient.affinity.Conf
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Container.AddPartition
import io.amient.affinity.core.config.CfgIntList
import io.amient.affinity.core.util.Reply

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.language.postfixOps

object Controller {

  final case class StartRebalance()

  final case class CreateGateway(handlerProps: Props) extends Reply[List[Int]]

  final case class GatewayCreated(httpPorts: List[Int])

  final case class FatalErrorShutdown(e: Throwable)

}

class Controller extends Actor {

  private val log = Logging.getLogger(context.system, this)

  import Controller._

  val system = context.system

  val conf = Conf(system.settings.config)

  val startupTimeout = conf.Affi.Node.StartupTimeoutMs().toLong milliseconds

  private var gatewayPromise: Promise[List[Int]] = null

  private val containers = scala.collection.mutable.Map[String, ActorRef]()

  import context.dispatcher

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
    case _: IllegalArgumentException ⇒ Resume
    case _: NoSuchElementException ⇒ Resume
    case _: NullPointerException ⇒ Resume
    case _: IllegalStateException ⇒ Resume
    case _: IllegalAccessException ⇒ Resume
    case _: SecurityException ⇒ Resume
    case _: NotImplementedError ⇒ Resume
    case _: UnsupportedOperationException ⇒ Resume
    case _: Exception ⇒ Restart
    case _: Throwable ⇒ Escalate
  }

  private def getOrCreateContainer(group: String): ActorRef = {
    if (!containers.contains(group)) {
      log.info(s"Starting container `$group`")
      val container = context.actorOf(Props(new Container(group)), name = group)
      containers.put(group, container)
    }
    containers(group)
  }

  override def preStart(): Unit = {
    super.preStart()
  }

  override def receive: Receive = {

    case Terminated(child) if (containers.contains(child.path.name)) => containers.remove(child.path.name)

    case _: StartRebalance =>
      if (conf.Affi.Node.Containers.isDefined) {
        conf.Affi.Node.Containers().asScala.toList.map {
          case (group: String, value: CfgIntList) =>
            val container = getOrCreateContainer(group)
            val partitions = value().asScala.map(_.toInt).toList
            val serviceClass = conf.Affi.Keyspace(group).PartitionClass()
            implicit val timeout = Timeout(startupTimeout)
            Await.result(Future.sequence(partitions.map{ p =>
              container ?? AddPartition(p, Props(serviceClass.newInstance()))
            }), startupTimeout)
        }
      }

    case request@CreateGateway(gatewayProps) => try {
      val gatewayRef = context.actorOf(gatewayProps, name = "gateway")
      context.watch(gatewayRef)
      gatewayPromise = Promise[List[Int]]()
      request(sender) ! gatewayPromise.future
      gatewayRef ! CreateGateway
    } catch {
      case _: InvalidActorNameException => request(sender) ! gatewayPromise.future
    }

    case Terminated(child) if (child.path.name == "gateway") =>
      if (!gatewayPromise.isCompleted) gatewayPromise.failure(new AkkaException("Gateway initialisation failed"))

    case GatewayCreated(ports) => if (!gatewayPromise.isCompleted) {
      if (ports.length > 0) log.info("Gateway online (with http)") else log.info("Gateway online (stream only)")
      gatewayPromise.success(ports)
    }

    case anyOther => log.warning("Unknown controller message " + anyOther)
  }

}
