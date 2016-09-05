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

import akka.actor.{Actor, ActorRef, Terminated}
import akka.event.Logging
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import io.amient.affinity.core.actor.Gateway.HttpExchange
import io.amient.affinity.core.cluster.Cluster
import io.amient.affinity.core.cluster.Coordinator.{AddService, RemoveService}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

object Gateway {

  final case class HttpExchange(request: HttpRequest, promise: Promise[HttpResponse])

}

abstract class Gateway(appConfig: Properties) extends Actor {

  protected val services = new ConcurrentHashMap[Class[_ <: Actor], Set[ActorRef]]

  val log = Logging.getLogger(context.system, this)

  val cluster = context.actorOf(new Cluster(appConfig).props(), name = "cluster")

  def describeServices = services.asScala.map { case (k, v) => (k.toString, v.map(_.path.toString)) }

  def describeRegions = {
    val t = 60 seconds
    implicit val timeout = Timeout(t)
    Await.result(cluster ? GetRoutees, t).asInstanceOf[Routees].routees.map(_.toString)
  }

  override def preStart(): Unit = {
    val t = 10 seconds
    implicit val timeout = Timeout(t)
    Await.ready(context.actorSelection(cluster.path).resolveOne(), t)
    //    context.watch(cluster)
    context.parent ! Controller.GatewayCreated()
  }

  override def postStop(): Unit = {
    super.postStop()
  }

  def service(actorClass: Class[_ <: Actor]): ActorRef = {
    //TODO handle missing service properly
    services.get(actorClass).head
  }

  object HTTP {
    def unapply(exchange: HttpExchange): Option[(HttpMethod, Path, Query, Promise[HttpResponse])] = {
      Some(exchange.request.method, exchange.request.uri.path, exchange.request.uri.query(), exchange.promise)
    }
  }

  object PATH {
    def unapplySeq(path: Path): Option[Seq[String]] = {
      @tailrec
      def r(p: Path, acc: Seq[String] = Seq()): Seq[String] =
        if (p.isEmpty) acc
        else if (p.startsWithSlash) r(p.tail, acc)
        else if (p.tail.isEmpty) acc :+ p.head.toString
        else r(p.tail, acc :+ p.head.toString)

      Some(r(path))
    }
  }

  object INT {
    def unapply(any: Any): Option[Int] = {
      try {
        Some(Integer.parseInt(any.toString))
      } catch {
        case e: NumberFormatException => None
      }
    }
  }

  object QUERY {
    def unapplySeq(query: Query): Option[Seq[(String, String)]] = Some(query.sortBy(_._1))
  }

  import context.dispatcher

  def handleError(status: StatusCode): HttpResponse

  def handle: Receive = {
    case null =>
  }

  final def receive: Receive = handle orElse {

    case e: HttpExchange => e.promise.success(handleError(NotFound))

    //Cluster Management queries
    case AddService(s) =>
      log.info("Adding Service " + s)
      val serviceClass = Class.forName(s.path.name).asSubclass(classOf[Actor])
      val actors = services.getOrDefault(serviceClass, Set[ActorRef]())
      services.put(serviceClass, actors + s)

    case RemoveService(s) =>
      log.info("Removeing Service" + s)
      val serviceClass = Class.forName(s.path.name).asSubclass(classOf[Actor])
      val actors = services.getOrDefault(serviceClass, Set[ActorRef]())
      services.put(serviceClass, actors - s)

    case m: AddRoutee => cluster ! m
    case m: RemoveRoutee => cluster ! m
    case m: GetRoutees =>
      val origin = sender()
      implicit val timeout = Timeout(60 seconds)
      cluster ? m onSuccess {
        case routees => origin ! routees
      }

    case Terminated(cluster) =>
      //          FIXME sometimes this doesn't restart the gateway
      throw new IllegalStateException("Cluster Actor terminated - must restart the gateway: " + cluster)

  }

}
