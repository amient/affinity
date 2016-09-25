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

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.event.Logging
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import io.amient.affinity.core.ack._
import io.amient.affinity.core.actor.Controller.GracefulShutdown
import io.amient.affinity.core.cluster.Coordinator.MasterStatusUpdate
import io.amient.affinity.core.http.{HttpExchange, HttpInterface, ResponseBuilder}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

object Gateway {
  final val CONFIG_HTTP_HOST = "affinity.node.gateway.http.host"
  final val CONFIG_HTTP_PORT = "affinity.node.gateway.http.port"
}

abstract class Gateway extends Actor {

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

  private val services = new ConcurrentHashMap[Class[_ <: Actor], ActorRef]

  val cluster = context.actorOf(Props(new Cluster()), name = "cluster")

  context.watch(cluster)

  import context.system

  private val httpInterface: HttpInterface = new HttpInterface(
    config.getString(Gateway.CONFIG_HTTP_HOST), config.getInt(Gateway.CONFIG_HTTP_PORT))

  httpInterface.bind(self)

  def describeServices = services.asScala.map { case (k, v) => (k.toString, v.path.toString) }

  def describeRegions = {
    val t = 60 seconds
    implicit val timeout = Timeout(t)
    Await.result(cluster ? GetRoutees, t).asInstanceOf[Routees].routees.map(_.toString).toList.sorted
  }

  override def preStart(): Unit = {
    log.info("starting gateway")
//    val t = 10 seconds
//    implicit val timeout = Timeout(t)
    //Await.ready(context.actorSelection(cluster.path).resolveOne(), t)
    context.parent ! Controller.GatewayCreated(httpInterface.getListenPort)

  }

  override def postStop(): Unit = {
    log.info("stopping gateway")
    httpInterface.close()
  }

  def service(actorClass: Class[_ <: Actor]): ActorRef = {
    services.get(actorClass) match {
      case null => throw new IllegalStateException(s"Service not available for $actorClass")
      case instance => instance
    }
  }

  def handleException: PartialFunction[Throwable, HttpResponse]= {
    case e: IllegalAccessError => HttpResponse(Forbidden)
    case e: NoSuchElementException => HttpResponse(NotFound)
    case e: IllegalArgumentException => HttpResponse(BadRequest)
    case e: NotImplementedError => e.printStackTrace(); HttpResponse(NotImplemented)
    case NonFatal(e) => e.printStackTrace(); HttpResponse(InternalServerError)
    case e => e.printStackTrace(); HttpResponse(ServiceUnavailable)
  }

  def fulfillAndHandleErrors(promise: Promise[HttpResponse], future: Future[Any], ct: ContentType)
                            (f: Any => HttpResponse)(implicit ctx: ExecutionContext) {
    promise.completeWith(future map f recover handleException)
  }

  def handle: Receive = {
    case null =>
  }

  final def receive: Receive = handle orElse {

    //no handler matched the HttpExchange
    case e: HttpExchange => e.promise.success(handleException(new NoSuchElementException))

    case MasterStatusUpdate("regions", add, remove) => ack[Unit](sender) {
      remove.foreach(ref => cluster ! RemoveRoutee(ActorRefRoutee(ref)))
      add.foreach(ref => cluster ! AddRoutee(ActorRefRoutee(ref)))
    }

    case MasterStatusUpdate("services", add, remove) => ack[Unit](sender) {
      add.foreach(ref => services.put(Class.forName(ref.path.name).asSubclass(classOf[Actor]), ref))
      remove.foreach(ref => services.remove(Class.forName(ref.path.name).asSubclass(classOf[Actor]), ref))
    }

    case GracefulShutdown() =>
      sender ! GracefulShutdown()
      context.stop(self)

    case Terminated(ref) =>
      throw new IllegalStateException("Cluster Actor terminated - must restart the gateway: " + ref)

  }


}
