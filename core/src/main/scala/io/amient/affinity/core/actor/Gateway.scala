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

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.{KeyManagerFactory, SSLContext}

import akka.actor.{Actor, ActorRef, PoisonPill, Props, Terminated}
import akka.event.Logging
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, UpgradeToWebSocket}
import akka.pattern.ask
import akka.routing._
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.{ByteString, Timeout}
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.Config
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller.GracefulShutdown
import io.amient.affinity.core.actor.Service.{CheckClusterAvailability, ClusterAvailability}
import io.amient.affinity.core.cluster.Coordinator
import io.amient.affinity.core.cluster.Coordinator.MasterStatusUpdate
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.http.{Encoder, HttpExchange, HttpInterface}
import io.amient.affinity.core.serde.avro.{AvroRecord, AvroSerde, AvroSerdeProxy}
import io.amient.affinity.core.util.ByteUtils
import org.apache.avro.util.ByteBufferInputStream

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.postfixOps
import scala.util.control.NonFatal

object Gateway {
  final val CONFIG_SERVICES = "affinity.service"

  final def CONFIG_SERVICE(group: String) = s"affinity.service.$group"

  final val CONFIG_GATEWAY_CLASS = "affinity.node.gateway.class"
  final val CONFIG_GATEWAY_SUSPEND_QUEUE_MAX_SIZE = "affinity.node.gateway.suspend.queue.max.size"
  final val CONFIG_GATEWAY_MAX_WEBSOCK_QUEUE_SIZE = "affinity.node.gateway.max.websocket.queue.size"
  final val CONFIG_GATEWAY_HTTP_HOST = "affinity.node.gateway.http.host"
  final val CONFIG_GATEWAY_HTTP_PORT = "affinity.node.gateway.http.port"

  final val CONFIG_GATEWAY_TLS_KEYSTORE_PKCS = "affinity.node.gateway.tls.keystore.standard"
  final val CONFIG_GATEWAY_TLS_KEYSTORE_PASSWORD = "affinity.node.gateway.tls.keystore.password"
  final val CONFIG_GATEWAY_TLS_KEYSTORE_RESOURCE = "affinity.node.gateway.tls.keystore.resource"
  final val CONFIG_GATEWAY_TLS_KEYSTORE_FILE = "affinity.node.gateway.tls.keystore.file"
}

abstract class Gateway extends Actor {

  import Gateway._

  private val log = Logging.getLogger(context.system, this)

  protected val config: Config = context.system.settings.config

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
  private val suspendedQueueMaxSize = config.getInt(CONFIG_GATEWAY_SUSPEND_QUEUE_MAX_SIZE)
  private val suspendedHttpRequestQueue = scala.collection.mutable.ListBuffer[HttpExchange]()

  import context.{dispatcher, system}

  private implicit val scheduler = context.system.scheduler

  val sslContext = if (!config.hasPath(CONFIG_GATEWAY_TLS_KEYSTORE_PASSWORD)) None else Some(SSLContext.getInstance("TLS"))
  sslContext.foreach { context =>
    log.info("Configuring SSL Context")
    val password = config.getString(CONFIG_GATEWAY_TLS_KEYSTORE_PASSWORD).toCharArray
    val ks = KeyStore.getInstance(config.getString(CONFIG_GATEWAY_TLS_KEYSTORE_PKCS))
    val is = if (config.hasPath(CONFIG_GATEWAY_TLS_KEYSTORE_RESOURCE)) {
      getClass.getClassLoader.getResourceAsStream(config.getString(CONFIG_GATEWAY_TLS_KEYSTORE_RESOURCE))
    } else {
      new FileInputStream(config.getString(CONFIG_GATEWAY_TLS_KEYSTORE_FILE))
    }
    try {
      ks.load(is, password)
    } finally {
      is.close()
    }
    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)
    context.init(keyManagerFactory.getKeyManagers, null, new SecureRandom)
  }

  private val httpInterface: HttpInterface = new HttpInterface(
    config.getString(CONFIG_GATEWAY_HTTP_HOST), config.getInt(CONFIG_GATEWAY_HTTP_PORT), sslContext)


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

  override def preStart(): Unit = {
    log.info("starting gateway")
    httpInterface.bind(self)
    //FIXME This probably causes the following akka warning: Message [java.lang.Integer] from Actor[akka://VPCAPI/deadLetters] to..
    context.parent ! Controller.GatewayCreated(httpInterface.getListenPort)
    services.values.foreach { case (coordinator, _, _) =>
      coordinator.watch(self, global = true)
    }
  }

  override def postStop(): Unit = {
    log.info("stopping gateway")
    httpInterface.close()
    services.values.foreach(_._1.unwatch(self))
  }

  def service(group: String): ActorRef = {
    services(group) match {
      case (_, null, _) => throw new IllegalStateException(s"Service not available for group $group")
      case (_, actorRef, _) => actorRef
    }
  }

  //  def service[X <: Actor](implicit tag: ClassTag[X]): ActorRef = {
  //    services(tag.runtimeClass) match {
  //      case null => throw new IllegalStateException(s"Service not available for ${tag.runtimeClass}")
  //      case instance => instance
  //    }
  //  }


  def handleException: PartialFunction[Throwable, HttpResponse] = {
    case e: NoSuchElementException => HttpResponse(NotFound, entity = if (e.getMessage == null) "" else e.getMessage)
    case e: IllegalArgumentException => HttpResponse(BadRequest, entity = if (e.getMessage == null) "" else e.getMessage)
    case e: scala.NotImplementedError => HttpResponse(NotImplemented, entity = if (e.getMessage == null) "" else e.getMessage)
    case e: UnsupportedOperationException => HttpResponse(NotImplemented, entity = if (e.getMessage == null) "" else e.getMessage)
    case _: IllegalStateException => HttpResponse(ServiceUnavailable)
    case NonFatal(e) => e.printStackTrace(); HttpResponse(InternalServerError)
  }

  def fulfillAndHandleErrors(promise: Promise[HttpResponse])(f: => Future[HttpResponse])
                            (implicit ctx: ExecutionContext) {
    promise.completeWith(f recover handleException)
  }

  def delegateAndHandleErrors(promise: Promise[HttpResponse], delegate: Future[Any])
                             (f: Any => HttpResponse)(implicit ctx: ExecutionContext) {
    promise.completeWith(delegate map f recover handleException)
  }

  def handleAsJson(response: Promise[HttpResponse], dataGenerator: => Any): Unit = try {
    val data = dataGenerator
    data match {
      case delegate: Future[_] => delegateAndHandleErrors(response, delegate) {
        case None => HttpResponse(NotFound)
        case Some(value) => Encoder.json(OK, value)
        case any => Encoder.json(OK, any)
      }
      case _ => response.success(try {
        data match {
          case None => HttpResponse(NotFound)
          case Some(value) => Encoder.json(OK, value)
          case other => Encoder.json(OK, other)
        }
      } catch {
        case NonFatal(e) => handleException(e)
      })
    }
  } catch {
    case e: Throwable => response.success(handleException(e))
  }

  def accept(response: Promise[HttpResponse], dataGenerator: => Any): Unit = try {
    val data = dataGenerator
    data match {
      case delegate: Future[_] => delegateAndHandleErrors(response, delegate) {
        case None => HttpResponse(NotFound)
        case _ => HttpResponse(Accepted)
      }
      case None => HttpResponse(NotFound)
      case _ => HttpResponse(Accepted)
    }
  } catch {
    case e: Throwable => response.success(handleException(e))
  }

  final def receive: Receive = manage orElse handle orElse {
    case exchange: HttpExchange => {
      //no handler matched the HttpExchange
      exchange.promise.success(handleException(new NoSuchElementException))
    }
  }

  def handle: Receive = {
    case null =>
  }

  private def manage: Receive = {

    case msg@MasterStatusUpdate(group, add, remove) => sender.reply(msg) {
      val service = services(group)._2
      remove.foreach(ref => service ! RemoveRoutee(ActorRefRoutee(ref)))
      add.foreach(ref => service ! AddRoutee(ActorRefRoutee(ref)))
      service ! CheckClusterAvailability(group)
    }


    case msg@ClusterAvailability(group, suspended) =>
      val (_, _, currentlySuspended) = services(group)
      if (currentlySuspended.get != suspended) {
        services(group)._3.set(suspended)
        val gatewayShouldBeSuspended = services.exists(_._2._3.get)
        if (gatewayShouldBeSuspended != handlingSuspended) {
          handlingSuspended = suspended
          if (!suspended) {
            log.info("Handling Resumed")
            context.system.eventStream.publish(msg)
            val reprocess = suspendedHttpRequestQueue.toList
            suspendedHttpRequestQueue.clear
            if (reprocess.length > 0) log.warning(s"Re-processing ${reprocess.length} suspended http requests")
            reprocess.foreach(handle(_))
          } else {
            log.warning("Handling Suspended")
          }
        }
      }

    case exchange: HttpExchange if (handlingSuspended) =>
      log.warning("Handling suspended, enqueuing request: " + exchange.request)
      if (suspendedHttpRequestQueue.size < suspendedQueueMaxSize) {
        suspendedHttpRequestQueue += exchange
      } else {
        handleException(new IllegalStateException)
      }

    case request@GracefulShutdown() => sender.reply(request) {
      context.stop(self)
    }

    case Terminated(ref) =>
      throw new IllegalStateException("Cluster Actor terminated - must restart the gateway: " + ref)

  }

}


trait WebSocketSupport extends Gateway {

  private val log = Logging.getLogger(context.system, this)

  private val afjs = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/affinity.js")).mkString

  private val serializers = SerializationExtension(context.system).serializerByIdentity

  private val avroSerde = AvroSerde.create(config)

  abstract override def handle: Receive = super.handle orElse {
    case http@HTTP(GET, PATH("affinity.js"), _, response) => response.success(Encoder.plain(OK, afjs))
  }

  protected def jsonWebSocket(upgrade: UpgradeToWebSocket, keyspace: ActorRef, stateStoreName: String, key: Any)
                             (pfCustomHandle: PartialFunction[JsonNode, Unit]): Future[HttpResponse] = {
    genericWebSocket(upgrade, keyspace, stateStoreName, key) {
      case text: TextMessage => log.info(text.getStrictText)
      case binary: BinaryMessage =>
        val buf = binary.getStrictData.asByteBuffer
        val json = Encoder.mapper.readValue(new ByteBufferInputStream(List(buf).asJava), classOf[JsonNode])
        pfCustomHandle(json)
    } {
      case None => TextMessage.Strict("null") //FIXME none type representation
      case Some(value) => TextMessage.Strict(Encoder.json(value))
      case direct: ByteString => BinaryMessage.Strict(direct)
      case other => TextMessage.Strict(Encoder.json(other))
    }
  }

  protected def avroWebSocket(http: HttpExchange, service: ActorRef, stateStoreName: String, key: Any)
                             (pfCustomHandle: PartialFunction[Any, Unit]): Unit = {
    http.request.header[UpgradeToWebSocket] match {
      case None => Future.failed(new IllegalArgumentException("WebSocket connection required"))
      case Some(upgrade) =>
        import context.dispatcher
        fulfillAndHandleErrors(http.promise) {
          avroWebSocket(upgrade, service, stateStoreName, key)(pfCustomHandle)
        }
    }
  }

  protected def avroWebSocket(upgrade: UpgradeToWebSocket, service: ActorRef, stateStoreName: String, key: Any)
                             (pfCustomHandle: PartialFunction[Any, Unit]): Future[HttpResponse] = {

    def buildSchemaPushMessage(schemaId: Int): ByteString = {
      val schemaBytes = avroSerde.schema(schemaId).get._2.toString(true).getBytes()
      val echoBytes = new Array[Byte](schemaBytes.length + 5)
      echoBytes(0) = 123
      ByteUtils.putIntValue(schemaId, echoBytes, 1)
      Array.copy(schemaBytes, 0, echoBytes, 5, schemaBytes.length)
      ByteString(echoBytes) //ByteString is a direct response over the push channel
    }

    /** Avro Web Socket Protocol:
      *
      * downstream TextMessage is considered a request for a schema of the given name
      * downstream BinaryMessage starts with a magic byte
      * 0   Is a binary avro message prefixed with BIG ENDIAN 32INT representing the schemId
      * 123 Is a schema request - binary buffer contains only BIG ENDIAN 32INT Schema and the client expects json schema to be sent back
      *       - thre response must also strat with 123 magic byte, followed by 32INT Schema ID and then schema json bytes
      *
      * upstream Option[Any] is expected to be an AvroRecord[_] and will be sent as binary message to the client
      * upstream ByteString will be sent as raw binary message to the client (used internally for schema request)
      * upstream any other type handling is not defined and will throw scala.MatchError
      */

    genericWebSocket(upgrade, service, stateStoreName, key) {
      case text: TextMessage =>
        try {
          buildSchemaPushMessage(avroSerde.schema(text.getStrictText).get)
        } catch {
          case NonFatal(e) => log.warning("Invalid websocket schema type request: " + text.getStrictText)
        }
      case binary: BinaryMessage =>
        try {
          val buf = binary.getStrictData.asByteBuffer
          buf.get(0) match {
            case 123 => buildSchemaPushMessage(schemaId = buf.getInt(1))
            case 0 => try {
              val record = AvroRecord.read(buf, avroSerde)
              val handleAvroClientMessage: PartialFunction[Any, Unit] = pfCustomHandle.orElse {
                case forwardToBackend: AvroRecord[_] => service ! forwardToBackend
              }
              handleAvroClientMessage(record)
            } catch {
              case NonFatal(e) => e.printStackTrace()
            }
          }
        } catch {
          case NonFatal(e) => log.warning("Invalid websocket binary avro message", e)
        }
    } {
      case direct: ByteString => BinaryMessage.Strict(direct) //ByteString as the direct response from above
      case None => BinaryMessage.Strict(ByteString()) //FIXME what should really be sent is a typed empty record which comes back to the API design issue of representing a zero-value of an avro record
      case Some(value: AvroRecord[_]) => BinaryMessage.Strict(ByteString(avroSerde.toBytes(value)))
      case value: AvroRecord[_] => BinaryMessage.Strict(ByteString(avroSerde.toBytes(value)))
    }
  }

  protected def genericWebSocket(upgrade: UpgradeToWebSocket, service: ActorRef, stateStoreName: String, key: Any)
                                (pfDown: PartialFunction[Message, Any])
                                (pfPush: PartialFunction[Any, Message]): Future[HttpResponse] = {
    import context.dispatcher
    implicit val scheduler = context.system.scheduler
    implicit val materializer = ActorMaterializer.create(context.system)
    implicit val timeout = Timeout(1 second)

    service ? (key, Partition.INTERNAL_CREATE_KEY_VALUE_MEDIATOR, stateStoreName) map {
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
            log.info("WebSocket Sink Closing")
            keyValueActor ! PoisonPill
          }

          override def receive: Receive = {
            case frontend: ActorRef => this.frontend = Some(frontend)
            case msg: Message => pfDown(msg) match {
              case Unit =>
              case response: ByteString => frontend.foreach(_ ! response)
              case other: Any => keyValueActor ! other
            }
          }
        }))
        val downMessageSink = Sink.actorRef[Message](clientMessageReceiver, PoisonPill)

        /**
          * Source.actorPublisher doesn't detect connections closed by the server so websockets
          * connections which are closed akka-server-side due to being idle leave zombie
          * flows materialized - akka/akka#21549
          * At the moment worked around by never closing idle connection
          * (in core/reference.conf akka.http.server.idle-timeout = infinite)
          */
        //FIXME seen websockets on the client being closed with correlated server log: Message [scala.runtime.BoxedUnit] from Actor[akka://VPCAPI/user/StreamSupervisor-0/flow-3-1-actorPublisherSource#1580470647] to Actor[akka://VPCAPI/deadLetters] was not delivered.
        val pushMessageSource = Source.actorPublisher[Message](Props(new ActorPublisher[Message] {

          final val maxBufferSize = context.system.settings.config.getInt(Gateway.CONFIG_GATEWAY_MAX_WEBSOCK_QUEUE_SIZE)

          override def preStart(): Unit = {
            context.watch(keyValueActor)
            keyValueActor ! self
            clientMessageReceiver ! self
          }

          override def postStop(): Unit = {
            log.info("WebSocket Source Closing")
          }

          private val buffer = new util.LinkedList[Message]()

          private def doPush(messages: Message*) = {
            while (buffer.size + messages.size > maxBufferSize) {
              buffer.pop()
            }
            messages.foreach(buffer.add)
            while (isActive && totalDemand > 0 && buffer.size > 0) {
              onNext(buffer.pop)
            }
          }

          override def receive: Receive = {
            case Terminated(source) => context.stop(self)
            case Request(_) => doPush()
            case pushMessage =>
              doPush(pfPush(pushMessage))
              //the keyvaluemediator which sent this message must be acked with a unit
              sender ! true
          }
        }))
        val flow = Flow.fromSinkAndSource(downMessageSink, pushMessageSource)
        upgrade.handleMessages(flow)
    }
  }

}