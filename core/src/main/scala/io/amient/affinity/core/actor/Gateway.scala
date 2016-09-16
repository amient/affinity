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

import akka.actor.{Actor, ActorRef, Terminated}
import akka.event.Logging
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.core.ack._
import io.amient.affinity.core.cluster.Cluster
import io.amient.affinity.core.cluster.Coordinator.{AddMaster, RemoveMaster}
import io.amient.affinity.core.http.{HttpExchange, HttpInterface}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object Gateway {
  final val CONFIG_HTTP_HOST = "affinity.node.gateway.http.host"
  final val CONFIG_HTTP_PORT = "affinity.node.gateway.http.port"
}
abstract class Gateway(config: Config) extends Actor {

  private val services = new ConcurrentHashMap[Class[_ <: Actor], Set[ActorRef]]

  val log = Logging.getLogger(context.system, this)

  val cluster = context.actorOf(new Cluster(config).props(), name = "cluster")

  import context.system

  val httpInterface: HttpInterface = new HttpInterface(
    config.getString(Gateway.CONFIG_HTTP_HOST), config.getInt(Gateway.CONFIG_HTTP_PORT))

  def describeServices = services.asScala.map { case (k, v) => (k.toString, v.map(_.path.toString)) }

  def describeRegions = {
    val t = 60 seconds
    implicit val timeout = Timeout(t)
    Await.result(cluster ? GetRoutees, t).asInstanceOf[Routees].routees.map(_.toString).toList.sorted
  }

  override def preStart(): Unit = {
    val t = 10 seconds
    implicit val timeout = Timeout(t)
    Await.ready(context.actorSelection(cluster.path).resolveOne(), t)
    context.watch(cluster)
    context.parent ! Controller.GatewayCreated()
    httpInterface.bind(self)
  }

  override def postStop(): Unit = {
    httpInterface.close()
  }

  def service(actorClass: Class[_ <: Actor]): ActorRef = {
    //TODO handle missing service properly
    services.get(actorClass).head // TODO round-robin or routing for services ?
  }

  def handleException: PartialFunction[Throwable, HttpResponse]

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

    //Cluster Management queries
    case AddMaster(group, ref) => ack[Unit](sender) {
      group match {
        case "regions" => cluster ! AddRoutee(ActorRefRoutee(ref))
        case "services" =>
          val serviceClass = Class.forName(ref.path.name).asSubclass(classOf[Actor])
          val actors = services.getOrDefault(serviceClass, Set[ActorRef]())
          if (!actors.contains(ref)) {
            services.put(serviceClass, actors + ref)
          }
      }
    }

    case RemoveMaster(group, ref) => ack[Unit](sender) {
      group match {
        case "regions" => cluster ! RemoveRoutee(ActorRefRoutee(ref))
        case "services" =>
          val serviceClass = Class.forName(ref.path.name).asSubclass(classOf[Actor])
          val actors = services.getOrDefault(serviceClass, Set[ActorRef]())
          services.put(serviceClass, actors - ref)
      }
    }

    case Terminated(ref) =>
      throw new IllegalStateException("Cluster Actor terminated - must restart the gateway: " + ref)

  }

}
