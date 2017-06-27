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
import akka.actor.{Actor, InvalidActorNameException, Props, Terminated}
import akka.event.Logging
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.util.Reply

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.language.postfixOps

object Controller {

  final case class CreateContainer(group: String, partitions: List[Int], partitionProps: Props) extends Reply[Unit]

  final case class ContainerOnline(group: String)

  final case class ServicesStarted()

  final case class CreateGateway(handlerProps: Props) extends Reply[Int]

  final case class GatewayCreated(httpPort: Int)

  final case class GracefulShutdown() extends Reply[Unit]

}

class Controller extends Actor {

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

  import Controller._

  val system = context.system

  private var gatewayPromise: Promise[Int] = null

  private val containers = scala.collection.mutable.Map[String, Promise[Unit]]()

  override def postStop(): Unit = {
    super.postStop()
  }

  import system.dispatcher

  implicit val scheduler = context.system.scheduler

  override def receive: Receive = {

    case request@CreateContainer(group, partitions, partitionProps) => sender.replyWith(request) {
      try {
        log.info(s"Creating Container for $group with partitions $partitions")
        context.actorOf(Props(new Container(group) {
          for (partition <- partitions) {
            context.actorOf(partitionProps, name = partition.toString)
          }
        }), name = group)
        val promise = Promise[Unit]()
        containers.put(group, promise)
        promise.future
      } catch {
        case e: InvalidActorNameException => containers(group).future
      }
    }

    case Terminated(child) if (containers.contains(child.path.name)) =>
      val promise = containers(child.path.name)
      if (!promise.isCompleted) promise.failure(new AkkaException("Container initialisation failed"))

    case ContainerOnline(group) => containers(group) match {
      case promise => if (!promise.isCompleted) containers(group).success(())
    }

    case request@CreateGateway(gatewayProps) => try {
      context.watch(context.actorOf(gatewayProps, name = "gateway"))
      gatewayPromise = Promise[Int]()
      sender.replyWith(request) {
        gatewayPromise.future
      }
    } catch {
      case e: InvalidActorNameException => sender.replyWith(request) {
        gatewayPromise.future
      }
    }

    case Terminated(child) if (child.path.name == "gateway") =>
      if (!gatewayPromise.isCompleted) gatewayPromise.failure(new AkkaException("Gateway initialisation failed"))

    case GatewayCreated(httpPort) => if (!gatewayPromise.isCompleted) {
      log.info("Gateway online (with http)")
      gatewayPromise.success(httpPort)
    }

    case ServicesStarted() => if (!gatewayPromise.isCompleted && !config.hasPath(GatewayHttp.CONFIG_GATEWAY_HTTP_HOST)) {
      log.info("Gateway online (without http)")
      gatewayPromise.success(-1)
    }

    case request@GracefulShutdown() => sender.replyWith(request) {
      implicit val timeout = Timeout(500 milliseconds)
      Future.sequence(context.children map { child =>
        log.info("Requesting GracefulShutdown from " + child)
        child.ack(GracefulShutdown())
      }) map (_ => system.terminate()) recover {
        case any =>
          any.printStackTrace()
          system.terminate()
      } map (_ => ())
    }

    case anyOther => log.warning("Unknown controller message " + anyOther)
  }

}
