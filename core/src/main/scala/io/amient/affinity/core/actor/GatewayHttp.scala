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

import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.security.{KeyStore, SecureRandom}
import java.util
import java.util.concurrent.ExecutionException
import javax.net.ssl.{KeyManagerFactory, SSLContext}

import akka.actor.{Actor, ActorRef, PoisonPill, Props, Terminated}
import akka.event.Logging
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, UpgradeToWebSocket}
import akka.pattern.ask
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.{ByteString, Timeout}
import com.typesafe.config.Config
import io.amient.affinity.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller.{CreateGateway, GracefulShutdown}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.http.{Encoder, HttpExchange, HttpInterface}
import io.amient.affinity.core.util.ByteUtils

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.postfixOps
import scala.util.control.NonFatal

object GatewayHttp {

  class Conf extends CfgStruct[Conf](Cfg.Options.IGNORE_UNKNOWN) {
    val Http = struct("affinity.node.gateway.http", new HttpConf, false)
    val Tls = struct("affinity.node.gateway.tls", new TlsConf, false)
  }

  class HttpConf extends CfgStruct[HttpConf] {

    val MaxWebSocketQueueSize = integer("max.websocket.queue.size", 100)
    val Host = string("host", true)
    val Port = integer("port", true)
    val Tls = struct("tls", new TlsConf, false)
  }

  class TlsConf extends CfgStruct[TlsConf] {
    val KeyStoreStandard = string("keystore.standard", "PKCS12")
    val KeyStorePassword = string("keystore.password", true)
    val KeyStoreResource = string("keystore.resource", false)
    val KeyStoreFile = string("keystore.file", false)
  }
}

trait GatewayHttp extends Gateway {

  private val log = Logging.getLogger(context.system, this)

  private val conf = Node.Conf(context.system.settings.config).Affi

  private val suspendedQueueMaxSize = conf.Gateway.SuspendQueueMaxSize()
  private val suspendedHttpRequestQueue = scala.collection.mutable.ListBuffer[HttpExchange]()

  import context.{dispatcher, system}

  private implicit val scheduler = context.system.scheduler

  private var isSuspended = true

  val sslContext = if (!conf.Gateway.Http.Tls.isDefined) None else Some(SSLContext.getInstance("TLS"))
  sslContext.foreach { context =>
    log.info("Configuring SSL Context")
    val password = conf.Gateway.Http.Tls.KeyStorePassword().toCharArray
    val ks = KeyStore.getInstance(conf.Gateway.Http.Tls.KeyStoreStandard())
    val is = if (conf.Gateway.Http.Tls.KeyStoreResource.isDefined) {
      val keystoreResource = conf.Gateway.Http.Tls.KeyStoreResource()
      log.info("Configuring SSL KeyStore from resouce: " + keystoreResource)
      getClass.getClassLoader.getResourceAsStream(keystoreResource)
    } else {
      val keystoreFileName = conf.Gateway.Http.Tls.KeyStoreFile()
      log.info("Configuring SSL KeyStore from file: " + keystoreFileName)
      new FileInputStream(keystoreFileName)
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
    conf.Gateway.Http.Host(), conf.Gateway.Http.Port(), sslContext)

  abstract override def preStart(): Unit = {
    super.preStart()
    log.info("Gateway starting")
    httpInterface.bind(self)
    context.parent ! Controller.GatewayCreated(httpInterface.getListenPort)
  }

  override def onClusterStatus(suspended: Boolean): Unit = if (isSuspended != suspended) {
    isSuspended = suspended
    log.info("Handling " + (if (suspended) "Suspended" else "Resumed"))
    if (!isSuspended) {
      val reprocess = suspendedHttpRequestQueue.toList
      suspendedHttpRequestQueue.clear
      if (reprocess.length > 0) log.warning(s"Re-processing ${reprocess.length} suspended http requests")
      reprocess.foreach(handle(_))
    }
  }

  abstract override def postStop: Unit = {
    super.postStop()
    log.info("Gateway stopping")
    httpInterface.close()
  }

  abstract override def manage: Receive = super.manage orElse {
    case msg@CreateGateway => context.parent ! Controller.GatewayCreated(httpInterface.getListenPort)

    case exchange: HttpExchange if (isSuspended) =>
      log.warning("Handling suspended, enqueuing request: " + exchange.request)
      if (suspendedHttpRequestQueue.size < suspendedQueueMaxSize) {
        suspendedHttpRequestQueue += exchange
      } else {
        new RuntimeException("Suspension queue overflow")
      }

    case request@GracefulShutdown() => sender.reply(request) {
      context.stop(self)
    }
  }

  override def unhandled: Receive = {
    case exchange: HttpExchange => {
      //no handler matched the HttpExchange
      exchange.promise.success(HttpResponse(NotFound, entity = "Server did not understand your request"))
    }
  }


  private def errorResponse(e: Throwable, status: StatusCode, message: String = "", headers: List[HttpHeader] = List()): HttpResponse = {
    log.error(e, "affinity default exception handler")
    HttpResponse(status, entity = if (e.getMessage == null) "" else e.getMessage, headers = headers)
  }

  def handleException: PartialFunction[Throwable, HttpResponse] = handleException(List())

  def handleException(headers: List[HttpHeader]): PartialFunction[Throwable, HttpResponse] = {
    case e: ExecutionException  => handleException(e.getCause)
    case e: NoSuchElementException => errorResponse(e, NotFound, if (e.getMessage == null) "" else e.getMessage, headers)
    case e: IllegalArgumentException => errorResponse(e, BadRequest, if (e.getMessage == null) "" else e.getMessage, headers)
    case e: IllegalStateException => errorResponse(e, Conflict, if (e.getMessage == null) "" else e.getMessage, headers)
    case e: java.lang.AssertionError => errorResponse(e, BadRequest, if (e.getMessage == null) "" else e.getMessage, headers)
    case e: scala.NotImplementedError => errorResponse(e, NotImplemented, if (e.getMessage == null) "" else e.getMessage, headers)
    case e: UnsupportedOperationException =>errorResponse(e, NotImplemented, if (e.getMessage == null) "" else e.getMessage, headers)
    case NonFatal(e) => errorResponse(e, InternalServerError, headers = headers)
  }

  def fulfillAndHandleErrors(promise: Promise[HttpResponse])(f: => Future[HttpResponse])
                            (implicit ctx: ExecutionContext) {
    promise.completeWith(f recover handleException)
  }

  def delegateAndHandleErrors(promise: Promise[HttpResponse], delegate: Future[Any])
                             (f: Any => HttpResponse)(implicit ctx: ExecutionContext) {
    promise.completeWith(delegate map f recover handleException)
  }

  def handleAsJson(response: Promise[HttpResponse], dataGenerator: => Any, headers: List[HttpHeader] = List()): Unit = try {
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
        case NonFatal(e) => handleException(headers)(e)
      })
    }
  } catch {
    case e: Throwable => response.success(handleException(headers)(e))
  }

  def accept(response: Promise[HttpResponse], dataGenerator: => Any, headers: List[HttpHeader] = List()): Unit = try {
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
    case e: Throwable => response.success(handleException(headers)(e))
  }
}


trait WebSocketSupport extends GatewayHttp {

  private val log = Logging.getLogger(context.system, this)

  private val config: Config = context.system.settings.config

  private val afjs = try {
    Some(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/affinity.js")).mkString)
  } catch {
    case NonFatal(e) =>
      log.warning("Could not load /affinity.js - it probably wasn't compiled from the affinity_node.js source, see README file for instructions: " + e.getMessage )
      None
  }

  private val serializers = SerializationExtension(context.system).serializerByIdentity

  private val avroSerde = AvroSerde.create(config)

  abstract override def handle: Receive = super.handle orElse {
    case http@HTTP(GET, PATH("affinity.js"), _, response) if afjs.isDefined => response.success(Encoder.plain(OK, afjs.get))
  }

  protected def jsonWebSocket(upgrade: UpgradeToWebSocket, keyspace: ActorRef, stateStoreName: String, key: Any)
                             (pfCustomHandle: PartialFunction[String, Unit]): Future[HttpResponse] = {
    genericWebSocket(upgrade, keyspace, stateStoreName, key) {
      case text: TextMessage => pfCustomHandle(text.getStrictText)
      case binary: BinaryMessage =>
        val buf = binary.getStrictData.asByteBuffer
        pfCustomHandle(StandardCharsets.UTF_8.decode(buf).toString())
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
      try {
        val schemaBytes = avroSerde.schema(schemaId).get.toString(true).getBytes()
        val echoBytes = new Array[Byte](schemaBytes.length + 5)
        echoBytes(0) = 123
        ByteUtils.putIntValue(schemaId, echoBytes, 1)
        Array.copy(schemaBytes, 0, echoBytes, 5, schemaBytes.length)
        ByteString(echoBytes) //ByteString is a direct response over the push channel
      } catch {
        case e: Throwable =>
          log.error(e, "Could not push schema object")
          throw e
      }
    }

    /** Avro Web Socket Protocol:
      *
      * downstream TextMessage is considered a request for a schema of the given name
      * downstream BinaryMessage starts with a magic byte
      * 0   Is a binary avro message prefixed with BIG ENDIAN 32INT representing the schemId
      * 123 Is a schema request - binary buffer contains only BIG ENDIAN 32INT Schema and the client expects json schema to be sent back
      *       - thre response must also strat with 123 magic byte, followed by 32INT Schema ID and then schema json bytes
      *
      * upstream Option[Any] is expected to be an AvroRecord and will be sent as binary message to the client
      * upstream ByteString will be sent as raw binary message to the client (used internally for schema request)
      * upstream any other type handling is not defined and will throw scala.MatchError
      */

    genericWebSocket(upgrade, service, stateStoreName, key) {
      case text: TextMessage =>
        val schemaFqn = text.getStrictText
        avroSerde.getCurrentSchema(schemaFqn) match {
          case Some((schemaId, _)) => buildSchemaPushMessage(schemaId)
          case None =>
            avroSerde.initialize(schemaFqn, AvroRecord.inferSchema(schemaFqn)) match {
              case schemaId :: Nil => buildSchemaPushMessage(schemaId)
              case _ => log.error(s"Invalid avro websocket schema type request: $schemaFqn")
            }
        }
      case binary: BinaryMessage =>
        try {
          val buf = binary.getStrictData.asByteBuffer
          buf.get(0) match {
            case 123 => buildSchemaPushMessage(schemaId = buf.getInt(1))
            case 0 => try {
              val record = AvroRecord.read(buf, avroSerde)
              val handleAvroClientMessage: PartialFunction[Any, Unit] = pfCustomHandle.orElse {
                case forwardToBackend: AvroRecord => service ! forwardToBackend
              }
              handleAvroClientMessage(record)
            } catch {
              case NonFatal(e) => log.error(e, "Invalid avro object received from the client")
            }
          }
        } catch {
          case NonFatal(e) => log.warning("Invalid websocket binary avro message", e)
        }
    } {
      case direct: ByteString => BinaryMessage.Strict(direct) //ByteString as the direct response from above
      case None => BinaryMessage.Strict(ByteString()) //FIXME what should really be sent is a typed empty record which comes back to the API design issue of representing a zero-value of an avro record
      case Some(value: AvroRecord) => BinaryMessage.Strict(ByteString(avroSerde.toBytes(value)))
      case value: AvroRecord => BinaryMessage.Strict(ByteString(avroSerde.toBytes(value)))
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
      case keyValueMediator: ActorRef =>
        /**
          * Sink.actorRef supports custom termination message which will be sent to the source
          * by the websocket materialized flow when the client closes the connection.
          * Using PoisonPill as termination message in combination with context.watch(source)
          * allows for closing the whole bidi flow in case the client closes the connection.
          */
        val clientMessageReceiver = context.actorOf(Props(new Actor {
          private var frontend: Option[ActorRef] = None

          override def postStop(): Unit = {
            log.debug("WebSocket Sink Closing")
            keyValueMediator ! PoisonPill
          }

          override def receive: Receive = {
            case frontend: ActorRef => this.frontend = Some(frontend)
            case msg: Message => pfDown(msg) match {
              case Unit =>
              case response: ByteString => frontend.foreach(_ ! response)
              case other: Any => keyValueMediator ! other
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

          val conf = Node.Conf(context.system.settings.config).Affi

          final val maxBufferSize = conf.Gateway.Http.MaxWebSocketQueueSize()

          override def preStart(): Unit = {
            context.watch(keyValueMediator)
            keyValueMediator ! self
            clientMessageReceiver ! self
          }

          override def postStop(): Unit = {
            log.debug("WebSocket Source Closing")
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