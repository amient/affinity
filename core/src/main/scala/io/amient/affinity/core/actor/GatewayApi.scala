/*
 * Copyright 2016-2017 Michal Harish, michal.harish@gmail.com
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

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorRef, Props, Terminated}
import akka.event.Logging
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller.GracefulShutdown
import io.amient.affinity.core.actor.Service.{CheckServiceAvailability, ClusterStatus, ServiceAvailability}
import io.amient.affinity.core.cluster.Coordinator
import io.amient.affinity.core.cluster.Coordinator.MasterStatusUpdate

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object GatewayApi {
  final val CONFIG_SERVICES = "affinity.service"

  final def CONFIG_SERVICE(group: String) = s"affinity.service.$group"
}

trait GatewayApi extends Gateway {

  import GatewayApi._

  private val log = Logging.getLogger(context.system, this)

  private val config: Config = context.system.settings.config

  private val services: Map[String, (Coordinator, ActorRef, AtomicBoolean)] =
    config.getObject(CONFIG_SERVICES).asScala.map(_._1).map { group =>
      val serviceConfig = config.getConfig(CONFIG_SERVICE(group))
      log.info(s"Expecting group $group of ${serviceConfig.getString(Service.CONFIG_NUM_PARTITIONS)} partitions")
      val service = context.actorOf(Props(new Service(serviceConfig)), name = group)
      val coordinator = Coordinator.create(context.system, group)
      context.watch(service)
      group -> (coordinator, service, new AtomicBoolean(true))
    }.toMap
  private var handlingSuspended = services.exists(_._2._3.get)

  import context.dispatcher

  private implicit val scheduler = context.system.scheduler

  def describeServices = services.map { case (group, (_, actorRef, _)) => (group, actorRef.path.toString) }

  def describeRegions: List[String] = {
    val t = 60 seconds
    implicit val timeout = Timeout(t)
    val routeeSets = Future.sequence {
      services.map { case (group, (_, actorRef, _)) =>
        actorRef ? GetRoutees map (_.asInstanceOf[Routees])
      }
    }
    Await.result(routeeSets, t).map(_.routees).flatten.map(_.toString).toList.sorted
  }

  abstract override def preStart(): Unit = {
    super.preStart()
    log.info("Services starting")
    context.parent ! Controller.ServicesStarted()
    services.values.foreach { case (coordinator, _, _) =>
      coordinator.watch(self, global = true)
    }
  }

  abstract override def postStop(): Unit = {
    super.postStop()
    log.info("Services stopping")
    services.values.foreach(_._1.unwatch(self))
  }

  def service(group: String): ActorRef = {
    services(group) match {
      case (_, null, _) => throw new IllegalStateException(s"Service not available for group $group")
      case (_, actorRef, _) => actorRef
    }
  }

  abstract override def manage: Receive = super.manage orElse {

    case msg@MasterStatusUpdate(group, add, remove) => sender.reply(msg) {
      val service = services(group)._2
      remove.foreach(ref => service ! RemoveRoutee(ActorRefRoutee(ref)))
      add.foreach(ref => service ! AddRoutee(ActorRefRoutee(ref)))
      service ! CheckServiceAvailability(group)
    }

    case msg@ServiceAvailability(group, suspended) =>
      val (_, _, currentlySuspended) = services(group)
      if (currentlySuspended.get != suspended) {
        services(group)._3.set(suspended)
        val gatewayShouldBeSuspended = services.exists(_._2._3.get)
        if (gatewayShouldBeSuspended != handlingSuspended) {
          handlingSuspended = suspended
          log.warning("Handling " + (if (suspended) "Suspended" else "Resumed"))
          context.system.eventStream.publish(msg)
          context.system.eventStream.publish(ClusterStatus(suspended))
        }
      }

    case request@GracefulShutdown() => sender.reply(request) {
      context.stop(self)
    }

    case Terminated(ref) =>
      throw new IllegalStateException("Cluster Actor terminated - must restart the gateway: " + ref)

  }
}
