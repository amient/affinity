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

import akka.actor.{Actor, ActorRef, PoisonPill, Props, Terminated}
import akka.event.Logging
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{Message, UpgradeToWebSocket}
import akka.pattern.ask
import akka.routing._
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller.GracefulShutdown
import io.amient.affinity.core.cluster.Coordinator.MasterStatusUpdate
import io.amient.affinity.core.http.{HttpExchange, HttpInterface}

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

  import context.system

  private val httpInterface: HttpInterface = new HttpInterface(
    config.getString(Gateway.CONFIG_HTTP_HOST), config.getInt(Gateway.CONFIG_HTTP_PORT))


  def describeServices = services.asScala.map { case (k, v) => (k.toString, v.path.toString) }

  def describeRegions = {
    val t = 60 seconds
    implicit val timeout = Timeout(t)
    Await.result(cluster ? GetRoutees, t).asInstanceOf[Routees].routees.map(_.toString).toList.sorted
  }

  override def preStart(): Unit = {
    log.info("starting gateway")
    httpInterface.bind(self)
    context.watch(cluster)
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

  def handleException: PartialFunction[Throwable, HttpResponse] = {
    case e: NoSuchElementException => HttpResponse(NotFound)
    case e: IllegalArgumentException => HttpResponse(BadRequest)
    case e: UnsupportedOperationException => HttpResponse(NotImplemented)
    case NonFatal(e) => e.printStackTrace(); HttpResponse(InternalServerError)
    case e => e.printStackTrace(); HttpResponse(ServiceUnavailable)
  }

  def fulfillAndHandleErrors(promise: Promise[HttpResponse])(f: => Future[HttpResponse])
                            (implicit ctx: ExecutionContext) {
    promise.completeWith(f recover handleException)
  }

  def delegateAndHandleErrors(promise: Promise[HttpResponse], delegate: Future[Any])
                             (f: Any => HttpResponse)(implicit ctx: ExecutionContext) {
    promise.completeWith(delegate map f recover handleException)
  }

  def handle: Receive = {
    case null =>
  }

  final def receive: Receive = handle orElse {

    //no handler matched the HttpExchange
    case e: HttpExchange => e.promise.success(handleException(new NoSuchElementException))

    case msg@MasterStatusUpdate("regions", add, remove) => sender.reply[Unit](msg) {
      remove.foreach(ref => cluster ! RemoveRoutee(ActorRefRoutee(ref)))
      add.foreach(ref => cluster ! AddRoutee(ActorRefRoutee(ref)))
    }

    case msg@MasterStatusUpdate("services", add, remove) => sender.reply[Unit](msg) {
      add.foreach(ref => services.put(Class.forName(ref.path.name).asSubclass(classOf[Actor]), ref))
      remove.foreach(ref => services.remove(Class.forName(ref.path.name).asSubclass(classOf[Actor]), ref))
    }

    case request@GracefulShutdown() => sender.reply(request) {
      context.stop(self)
    }

    case Terminated(ref) =>
      throw new IllegalStateException("Cluster Actor terminated - must restart the gateway: " + ref)

  }

  /*
   * WebSocket support methods
   */

  protected def keyValueWebSocket(upgrade: UpgradeToWebSocket, stateStoreName: String, key: Any)
                                 (pfPush: PartialFunction[Any, Message])
                                 (pfDown: PartialFunction[Message, Any]): Future[HttpResponse] = {
    import context.dispatcher
    implicit val scheduler = system.scheduler
    implicit val materializer = ActorMaterializer.create(system)
    implicit val timeout = Timeout(1 second)

    cluster ? (key, stateStoreName, Partition.INTERNAL_KEY_VALUE_OBSERVER) map {
      case keyValueActor: ActorRef =>

        /**
          * Sink.actorRef supports custom termination message which will be sent to the source
          * by the websocket materialized flow when the client closes the connection.
          * Using PoisonPill as termination message in combination with context.watch(source)
          * allows for closing the whole bidi flow in case the client closes the connection.
          */
        val clientMessageReceiver = context.actorOf(Props(new Actor {
          private var frontend: Option[ActorRef] = None

          override def postStop(): Unit = {
            keyValueActor ! PoisonPill
          }

          override def receive: Receive = {
            case frontend: ActorRef => this.frontend = Some(frontend)
            case msg: Message => pfDown(msg) match {
              case Unit =>
              case response: Array[Byte] => frontend.foreach(_ ! response)
              case other: Any => keyValueActor ! other
            }
          }
        }))
        val downMessageSink = Sink.actorRef[Message](clientMessageReceiver, PoisonPill)

        /**
          * Source.actorPublisher doesn't detect connections closed by the server so websockets
          * connections which are closed akka-server-side due to being idle leave zombie
          * flows materialized.
          */
        //TODO akka/akka#21549 - at the moment worked around by never closing idle connections
        //  (in core/refernce.conf akka.http.server.idle-timeout = infinite)
        val pushMessageSource = Source.actorPublisher[Message](Props(new ActorPublisher[Message] {

          override def preStart(): Unit = {
            context.watch(keyValueActor)
            keyValueActor ! self
            clientMessageReceiver ! self
          }

          override def receive: Receive = {
            case Request(_) =>
            case Terminated(source) => context.stop(self)
            case msg => sender ! onNext(pfPush(msg)) //will throw an exception if no messages well demanded
          }
        }))
        val flow = Flow.fromSinkAndSource(downMessageSink, pushMessageSource)
        upgrade.handleMessages(flow)
    }
  }
}
