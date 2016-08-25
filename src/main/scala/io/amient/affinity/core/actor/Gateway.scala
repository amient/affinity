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

import akka.actor.{Actor, Status, Terminated}
import akka.event.Logging
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import io.amient.affinity.core.HttpExchange
import io.amient.affinity.core.actor.Partition.{CollectUserInput, KillNode, SimulateError}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.control.NonFatal

object Gateway {

  final val CONFIG_NUM_PARTITIONS = "num.partitions"


}

class Gateway(appConfig: Properties) extends Actor {

  val log = Logging.getLogger(context.system, this)

  import Gateway._

  val numPartitions = appConfig.getProperty(CONFIG_NUM_PARTITIONS).toInt

  val partitioner = context.actorOf(PartitionedGroup(numPartitions).props(), name = "partitioner")

  import context.dispatcher

  context.watch(partitioner)

  override def preStart(): Unit = {
    Await.ready(context.actorSelection(partitioner.path).resolveOne()(10 seconds), 10 seconds)
    context.parent ! Controller.GatewayCreated()
  }

  def receive = {

    //TODO create an annotation which handles HttpExchange matching by parameter and fulfillment by implementation

    //non-delegate response
    case HttpExchange(GET, Uri.Path("/") | Uri.Path(""), _, _, promise) =>
      promise.success(HttpResponse(
        entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Hello World!</h1>")))


    //delegate query path
    case HttpExchange(GET, uri, _, _, promise) => uri match {

      case Uri.Path("/error") =>
        implicit val timeout = Timeout(1 second)
        //TODO the default supervisor strategy will try to restart gateway if exception occurs here
        partitioner ! SimulateError(new IllegalStateException)
        promise.success(HttpResponse(status = StatusCodes.Accepted))

      case Uri.Path("/kill") =>
        implicit val timeout = Timeout(1 second)
        uri.query() match {
          case Seq(("p", x)) if (x.toInt >= 0) =>
            fulfillAndHandleErrors(promise, partitioner ? KillNode(x.toInt)) {
              case any => HttpResponse(status = StatusCodes.Accepted)
            }
          case _ => badRequest(promise, "query string param p must be >=0")
        }

      case Uri.Path("/hello") =>
        implicit val timeout = Timeout(60 seconds)
        fulfillAndHandleErrors(promise, partitioner ? CollectUserInput(uri.queryString().getOrElse(null))) {
          case justText: String => HttpResponse(
            entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>" + justText + "</h1>"))

          case s => HttpResponse(status = StatusCodes.NotAcceptable,
            entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Can't give you that: " + s.getClass + "</h1>"))
        }

      case Uri.Path("/hi") =>
        promise.success(HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
          "<html><body><h2>Please wait..</h2>" +
            "<script>setTimeout(\"location.href = '/hello?" + uri.query() + "';\",100);" +
            "</script></body></html>")))

      case _ =>
        promise.success(HttpResponse(status = StatusCodes.NotFound,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Haven't got that</h1>")))
    }


    //TODO command path (POST)


    case HttpExchange(_, _, _, _, promise) =>
      promise.success(HttpResponse(status = StatusCodes.MethodNotAllowed,
        entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Can't do that here </h1>")))

    //Management queries
    case m: AddRoutee =>
      log.info("Adding partition: " + m.routee)
      partitioner ! m

    case m: RemoveRoutee =>
      log.info("Removing partition: " + m.routee)
      partitioner ! m

    case m: GetRoutees =>
      val origin = sender()
      implicit val timeout = Timeout(60 seconds)
      partitioner ? m onSuccess { case routees => origin ! routees }

    case Terminated(partitioner) =>
      throw new IllegalStateException("Partitioner terminated - must restart the gateway")

    case _ => sender ! Status.Failure(new IllegalArgumentException)

  }

  def badRequest(promise: Promise[HttpResponse], message: String) = {
    promise.success(HttpResponse(status = StatusCodes.BadRequest,
      entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>$message</h1>")))
  }
  def fulfillAndHandleErrors(promise: Promise[HttpResponse], future: Future[Any])(f: Any => HttpResponse) = {
    promise.completeWith(handleErrors(future, f))
  }

  def handleErrors(future: Future[Any], f: Any => HttpResponse): Future[HttpResponse] = {
    future map(f) recover {
      case e: IllegalArgumentException =>
        log.error("Gateway contains bug! ", e)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> Eeek! We have a bug..</h1>"))

      case NonFatal(e) =>
        log.error("Cluster encountered failure ", e.getMessage)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
            "<h1> Well, something went wrong but we should be back..</h1>"))

      case e =>
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
            "<h1> Something is seriously wrong with our servers..</h1>"))

    }
  }

}
