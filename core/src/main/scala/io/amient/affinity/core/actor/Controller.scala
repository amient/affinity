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

import akka.actor.{Actor, InvalidActorNameException, Props, Terminated}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.ack._
import io.amient.affinity.core.cluster.{Coordinator, Node}

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object Controller {

  final case class CreateRegion(partitionProps: Props)

  final case class ContainerOnline(group: String)

  final case class CreateGateway(handlerProps: Props)

  final case class CreateServiceContainer(services: Seq[Props])

  final case class GatewayCreated(httpPort: Int)

  final case class GracefulShutdown()

}

class Controller extends Actor {

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

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

  private var gatewayPromise: Promise[Int] = null
  private var regionPromise: Promise[Unit] = null
  private var servicesPromise: Promise[Unit] = null

  override def postStop(): Unit = {
    regionCoordinator.close()
    serviceCoordinator.close()
    super.postStop()
  }

  override def receive: Receive = {

    case CreateServiceContainer(services) =>
      try {
        context.actorOf(Props(new Container(serviceCoordinator, "services") {
          services.foreach { serviceProps =>
            context.actorOf(serviceProps, serviceProps.actorClass().getName)
          }
        }), name = "services")
        servicesPromise = Promise[Unit]()
        replyWith(sender) {
          servicesPromise.future
        }
      } catch {
        case e: InvalidActorNameException => replyWith(sender) {
          servicesPromise.future
        }
      }

    case ContainerOnline("services") => servicesPromise.success(())

    case CreateRegion(partitionProps) =>
      val origin = sender
      try {
        context.actorOf(Props(new Container(regionCoordinator, "region") {
          val partitions = config.getIntList(Node.CONFIG_PARTITION_LIST).asScala
          for (partition <- partitions) {
            context.actorOf(partitionProps, name = partition.toString)
          }
        }), name = "region")
        regionPromise = Promise[Unit]()
        replyWith(origin) {
          regionPromise.future
        }
      } catch {
        case e: InvalidActorNameException => replyWith(origin) {
          gatewayPromise.future
        }
      }

    //FIXME when region shuts down due to partition exception CreateRegion is never called again and the promise will be 'already completed'
    case ContainerOnline("region") => regionPromise.success(())


    case CreateGateway(gatewayProps) =>
      try {
        context.actorOf(gatewayProps, name = "gateway")
        gatewayPromise = Promise[Int]()
        replyWith(sender) {
          gatewayPromise.future
        }
      } catch {
        case e: InvalidActorNameException => replyWith(sender) {
          gatewayPromise.future
        }
      }

    case GatewayCreated(httpPort) =>
      log.info("gateway is online " + sender)
      regionCoordinator.watch(sender, global = true)
      serviceCoordinator.watch(sender, global = true)
      context.watch(sender)
      //FIXME when gateway gets restarted CreateGateway is never called again and the promise will be 'already completed'
      gatewayPromise.success(httpPort)

    case GracefulShutdown() => replyWith(sender) {
      implicit val timeout = Timeout(500 milliseconds)
      context.actorSelection("gateway") ? GracefulShutdown() recover {
        case any => system.terminate()
      }
    }

    case Terminated(gateway) => val gateway = sender
      //FIXME this code doesn't differntiate between termination caused by GracefulShutdown and gateway failure
      regionCoordinator.unwatch(gateway)
      serviceCoordinator.unwatch(gateway)
      log.info("graceful shutdown completed, terminating actor system")
      context.system.terminate()

    case anyOther => log.warning("Unknown controller message " + anyOther)
  }

}
