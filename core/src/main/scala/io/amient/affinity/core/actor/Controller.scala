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

import akka.actor.{Actor, InvalidActorNameException, PoisonPill, Props, Terminated}
import akka.event.Logging
import io.amient.affinity.core.ack._
import io.amient.affinity.core.cluster.Coordinator

object Controller {

  final case class CreateRegion(partitionProps: Props)

  final case class CreateGateway(handlerProps: Props)

  final case class CreateServiceContainer(services: Seq[Props])

  final case class GatewayCreated()

  final case class GracefulShutdown()

}

class Controller extends Actor {

  val log = Logging.getLogger(context.system, this)

  import Controller._

  implicit val system = context.system

  //controller terminates the system so cannot use system.dispatcher for Futures execution
  import scala.concurrent.ExecutionContext.Implicits.global

  val regionCoordinator = try {
    Coordinator.create(system, "regions")
  } catch {
    case e: Throwable =>
      system.terminate() onComplete { _ =>
        e.printStackTrace()
        System.exit(10)
      }
      throw e
  }

  val serviceCoordinator = try {
    Coordinator.create(system, "services")
  } catch {
    case e: Throwable =>
      system.terminate() onComplete { _ =>
        e.printStackTrace()
        System.exit(11)
      }
      throw e
  }

  override def postStop(): Unit = {
    regionCoordinator.close()
    serviceCoordinator.close()
    super.postStop()
  }

  override def receive: Receive = {

    case CreateServiceContainer(services) => ack[Unit](sender) {
      try {
        context.actorOf(Props(new Container(serviceCoordinator, "services") {
          services.foreach { serviceProps =>
            context.actorOf(serviceProps, serviceProps.actorClass().getName)
          }
        }), name = "services")
      } catch {
        case e: InvalidActorNameException => throw AckDuplicate(e)
      }
    }

    case CreateRegion(partitionProps) => ack[Unit](sender) {
      try {
        context.actorOf(Props(new Region(regionCoordinator, partitionProps)), name = "region")
      } catch {
        case e: InvalidActorNameException => throw AckDuplicate(e)
      }
    }

    case CreateGateway(gatewayProps) => ack[Unit](sender) {
      try {
        context.actorOf(gatewayProps, name = "gateway")
      } catch {
        case e: InvalidActorNameException => throw AckDuplicate(e)
      }
    }

    case GatewayCreated() =>
      log.info("gateway is online " + sender)
      regionCoordinator.watch(sender, global = true)
      serviceCoordinator.watch(sender, global = true)
      context.watch(sender)

    case GracefulShutdown() => ack(sender) {
      context.actorSelection("gateway") ! PoisonPill
    }

    case Terminated(gateway) => val gateway = sender
      regionCoordinator.unwatch(gateway)
      serviceCoordinator.unwatch(gateway)
      log.info("graceful shutdown completed, terminating actor system")
      context.system.terminate()

    case anyOther => log.warning("Unknown controller message " + anyOther)
  }

}
