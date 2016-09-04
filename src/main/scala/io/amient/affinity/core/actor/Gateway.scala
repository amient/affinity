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
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.event.Logging
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import io.amient.affinity.core.cluster.{Cluster, Coordinator}

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

import scala.collection.JavaConverters._

object Gateway {

  final case class HttpExchange(request: HttpRequest, promise: Promise[HttpResponse])

}

class Gateway(appConfig: Properties) extends Actor {

  val log = Logging.getLogger(context.system, this)

  import context.dispatcher

  import Coordinator._

  val cluster = context.actorOf(new Cluster(appConfig).props(), name = "cluster")

  def describeRegions = {
    val t = 60 seconds
    implicit val timeout = Timeout(t)
    Await.result(cluster ? GetRoutees, t).asInstanceOf[akka.routing.Routees].routees.map(_.toString)
  }
  private val services = new ConcurrentHashMap[Class[_ <: Actor], Set[ActorRef]]

  def describeServices = services.asScala.map{ case (k,v) => (k.toString, v.map(_.path.toString))}

  context.watch(cluster)

  override def preStart(): Unit = {
    val t = 10 seconds
    implicit val timeout = Timeout(t)
    Await.ready(context.actorSelection(cluster.path).resolveOne(), t)
    context.parent ! Controller.GatewayCreated()
  }

  def service(actorClass:Class[_ <: Actor]): ActorRef = {
    //TODO handle missing service properly
    services.get(actorClass).head
  }

  def receive:  PartialFunction[Any, Unit] = {

    //Cluster Management queries
    case AddService(s) =>
      val serviceClass = Class.forName(s.path.name).asSubclass(classOf[Actor])
      val actors = services.getOrDefault(serviceClass, Set[ActorRef]())
      services.put(serviceClass, actors + s)

    case RemoveService(s) =>
      val serviceClass = Class.forName(s.path.name).asSubclass(classOf[Actor])
      val actors = services.getOrDefault(serviceClass, Set[ActorRef]())
      services.put(serviceClass, actors - s)

    case m: AddRoutee => cluster ! m
    case m: RemoveRoutee => cluster ! m
    case m: GetRoutees =>
      val origin = sender()
      //TODO this timeout should be configurable as it dependes on the size of the cluster and coordinator implementation
      implicit val timeout = Timeout(60 seconds)
      cluster ? m onSuccess { case routees => origin ! routees }

    case Terminated(cluster) =>
      throw new IllegalStateException("Cluster Actor terminated - must restart the gateway")

  }

}
