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
import java.security.{KeyStore, SecureRandom}
import java.util
import java.util.Optional
import java.util.concurrent.ExecutionException
import javax.net.ssl.{KeyManagerFactory, SSLContext}

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.{ByteString, Timeout}
import com.typesafe.config.Config
import io.amient.affinity.Conf
import io.amient.affinity.avro.record.{AvroRecord, AvroSerde}
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller.{CreateGateway, GracefulShutdown}
import io.amient.affinity.core.actor.Partition.RegisterMediatorSubscriber
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.http._
import io.amient.affinity.core.util.ByteUtils

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.util.control.NonFatal

object GatewayHttp {

  object GatewayConf extends GatewayConf {
    override def apply(config: Config) = new GatewayConf().apply(config)
  }

  class GatewayConf extends CfgStruct[GatewayConf](Cfg.Options.IGNORE_UNKNOWN) {
    val Http = struct("affinity.node.gateway.http", new HttpConf)
    val Tls = struct("affinity.node.gateway.tls", new TlsConf)
  }

  class HttpConf extends CfgStruct[HttpConf] {

    val MaxWebSocketQueueSize = integer("max.websocket.queue.size", 100)
    val Host = string("host", true)
    val Port = integer("port", true)
    val Tls = struct("tls", new TlsConf)
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

  private val conf = Conf(context.system.settings.config).Affi

  private val suspendedQueueMaxSize = conf.Node.Gateway.SuspendQueueMaxSize()
  private val suspendedHttpRequestQueue = scala.collection.mutable.ListBuffer[HttpExchange]()

  import context.{dispatcher, system}

//  private implicit val scheduler = context.system.scheduler

  private var isSuspended = true

  val sslContext = if (!conf.Node.Gateway.Http.Tls.isDefined) None else Some(SSLContext.getInstance("TLS"))
  sslContext.foreach { context =>
    log.info("Configuring SSL Context")
    val password = conf.Node.Gateway.Http.Tls.KeyStorePassword().toCharArray
    val ks = KeyStore.getInstance(conf.Node.Gateway.Http.Tls.KeyStoreStandard())
    val is = if (conf.Node.Gateway.Http.Tls.KeyStoreResource.isDefined) {
      val keystoreResource = conf.Node.Gateway.Http.Tls.KeyStoreResource()
      log.info("Configuring SSL KeyStore from resouce: " + keystoreResource)
      getClass.getClassLoader.getResourceAsStream(keystoreResource)
    } else {
      val keystoreFileName = conf.Node.Gateway.Http.Tls.KeyStoreFile()
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
    conf.Node.Gateway.Http.Host(), conf.Node.Gateway.Http.Port(), sslContext)

  lazy private val listener = httpInterface.bind(self)

  abstract override def preStart(): Unit = {
    super.preStart()
    log.info("Gateway starting")
    context.parent ! Controller.GatewayCreated(listener.getPort)
  }

  abstract override def postStop(): Unit = try {
    httpInterface.close()
    log.info("Http Interface closed")
    Http().shutdownAllConnectionPools()
  } finally {
    super.postStop()
  }

  abstract override def onClusterStatus(suspended: Boolean): Unit = {
    super.onClusterStatus(suspended)
    if (isSuspended != suspended) {
      isSuspended = suspended
      log.info("Handling " + (if (suspended) "Suspended" else "Resumed"))
      if (!isSuspended) {
        val reprocess = suspendedHttpRequestQueue.toList
        suspendedHttpRequestQueue.clear
        if (reprocess.length > 0) log.warning(s"Re-processing ${reprocess.length} suspended http requests")
        reprocess.foreach(handle(_))
      }
    }
  }

  abstract override def manage: Receive = super.manage orElse {
    case msg@CreateGateway => context.parent ! Controller.GatewayCreated(listener.getPort)

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

  def accept(response: Promise[HttpResponse], dataGenerator: => Any, headers: List[HttpHeader] = List()): Unit = try {
    val data = dataGenerator
    data match {
      case delegate: Future[_] => handleWith(response) {
        delegate map {
          case None => HttpResponse(NotFound)
          case _ => HttpResponse(Accepted)
        }
      }
      case None => HttpResponse(NotFound)
      case _ => HttpResponse(Accepted)
    }
  } catch {
    case e: Throwable => response.success(handleException(headers)(e))
  }

  def handleAsText(response: Promise[HttpResponse], dataGenerator: => Any, headers: List[HttpHeader] = List()): Unit = {
    handleAs(response, dataGenerator, headers)(data => Encoder.text(OK, data))
  }

  def handleAsJson(response: Promise[HttpResponse], dataGenerator: => Any, headers: List[HttpHeader] = List()): Unit = {
    handleAs(response, dataGenerator, headers)(data => Encoder.json(OK, data))
  }

  def handleAs(response: Promise[HttpResponse], dataGenerator: => Any, headers: List[HttpHeader] = List())(f: (Any) => HttpResponse): Unit = try {
    val data = dataGenerator
    data match {
      case delegate: Future[_] => handleWith(response) {
        delegate map {
          case None => HttpResponse(NotFound)
          case Some(value) => f(value)
          case any => f(any)
        }
      }
      case _ => response.success(try {
        data match {
          case None => HttpResponse(NotFound)
          case Some(value) => f(value)
          case other => f(other)
        }
      } catch {
        case NonFatal(e) => handleException(headers)(e)
      })
    }
  } catch {
    case e: Throwable => response.success(handleException(headers)(e))
  }


  def handleWith(promise: Promise[HttpResponse])(f: => Future[HttpResponse])(implicit ctx: ExecutionContext): Unit = {
    promise.completeWith(f recover handleException)
  }


  def handleException: PartialFunction[Throwable, HttpResponse] = handleException(List())

  def handleException(headers: List[HttpHeader]): PartialFunction[Throwable, HttpResponse] = {
    case e@RequestException(status) => errorResponse(e, StatusCodes.custom(status.intValue().toString.take(3).toInt, status.reason, status.defaultMessage), headers)
    case e: ExecutionException => handleException(e.getCause)
    case e: NoSuchElementException => errorResponse(e, NotFound, headers)
    case e: IllegalArgumentException => errorResponse(e, BadRequest, headers)
    case e: IllegalStateException => errorResponse(e, Conflict, headers)
    case e: IllegalAccessException => errorResponse(e, Forbidden)
    case e: SecurityException => errorResponse(e, Unauthorized)
    case e: NotImplementedError => errorResponse(e, NotImplemented, headers)
    case e: UnsupportedOperationException => errorResponse(e, NotImplemented, headers)
    case NonFatal(e) => errorResponse(e, InternalServerError, headers = headers)
  }

  private def errorResponse(e: Throwable, status: StatusCode, headers: List[HttpHeader] = List()): HttpResponse = {
    log.error(e, s"${status} - default http error handler")
    status.defaultMessage() match {
      case null => HttpResponse(status, headers = headers)
      case message => HttpResponse(status, entity = message, headers = headers)
    }
  }

}


trait WebSocketSupport extends GatewayHttp {

  private val log = Logging.getLogger(context.system, this)

  private val config: Config = context.system.settings.config

  private val afjs = try {
    Some(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/affinity.js")).mkString)
  } catch {
    case NonFatal(e) =>
      log.warning("Could not load /affinity.js - it probably wasn't compiled from the affinity_node.js source, see README file for instructions: " + e.getMessage)
      None
  }

//  private val serializers = SerializationExtension(context.system).serializerByIdentity

  private val avroSerde = AvroSerde.create(config)

  private implicit val materializer: ActorMaterializer = ActorMaterializer.create(context.system)

  abstract override def handle: Receive = super.handle orElse {
    case http@HTTP(GET, PATH("affinity.js"), _, response) if afjs.isDefined => response.success(Encoder.text(OK, afjs.get))
  }

  /**
    * jsonWebSocket:
    * +
    * |
    * |                           send:TEXT   +----------+  send:JSON
    * |                 +------+  send:BIN    |DOWNSTREAM+---------------+
    * |                 |      +------------> |ACTOR     |  close   +-----------------------------+
    * |                 |      |    close     +-----+----+          |    |                        |
    * |                 | WEB  |                    |               | +--v------+     +---------+ |
    * |     CLIENT <---->SOCKET|                push|close          | |MEDIATOR +----->KEY|VALUE| |
    * |                 |      |                    |               | +--+------+     +---------+ |
    * |                 |      |     push     +-----v----+          |    |  STATE PARTITION       |
    * |                 |      | <------------+UPSTREAM  |   push   +-----------------------------+
    * |                 +------+     TEXT     |ACTOR     +---------------+
    * |                                       +----------+   JSON
    * +
    *
    * @param exchange web socket echange as captured in the http interface
    * @param mediator key-value mediator create using connectKeyValueMediator()
    */
  def jsonWebSocket(exchange: WebSocketExchange, mediator: ActorRef): Unit = {
    customWebSocket(exchange, new DownstreamActor {
      override def onOpen(upstream: ActorRef): Unit = mediator ! RegisterMediatorSubscriber(upstream)

      override def onClose(upstream: ActorRef): Unit = mediator ! PoisonPill

      override def receiveMessage(upstream: ActorRef): PartialFunction[Message, Unit] = {
        case text: TextMessage => mediator ! Decoder.json(text.getStrictText)
        case binary: BinaryMessage => mediator ! Decoder.json(binary.dataStream)
      }

    }, new UpstreamActor {
      override def handle: Receive = {
        case None => push(TextMessage.Strict("{}"))
        case Some(value) => push(TextMessage.Strict(Encoder.json(value)))
        case opt: Optional[_] if !opt.isPresent => push(TextMessage.Strict("{}"))
        case opt: Optional[_] => push(TextMessage.Strict(Encoder.json(opt.get)))
        case other => push(TextMessage.Strict(Encoder.json(other)))
      }
    })
  }

  /**
    * avroWebSocket:
    * +
    * |                           send:TEXT   +----------+
    * |                 +------+  send:BIN    |DOWNSTREAM|  send:AVRO
    * |                 |      +------------> |ACTOR     +---------------+
    * |                 |      |    close     +-----+----+  close   +-----------------------------+
    * |                 |      |                    |               |    |                        |
    * |                 | WEB  |                push|               | +--v------+     +---------+ |
    * |     CLIENT <----+SOCKET|              SCHEMA|close          | |MEDIATOR +----->KEY|VALUE| |
    * |                 |      |                    |               | +--+------+     +---------+ |
    * |                 |      |                    |               |    |  STATE PARTITION       |
    * |                 |      |     push     +-----v----+   push   +-----------------------------+
    * |                 |      | <------------+UPSTREAM  +---------------+
    * |                 +------+    BINARY    |ACTOR     |   AVRO
    * +
    * +----------+
    * Avro Web Socket Protocol:
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
    *
    * @param exchange web socket echange as captured in the http interface
    * @param mediator key-value mediator create using connectKeyValueMediator()
    */
  def avroWebSocket(exchange: WebSocketExchange, mediator: ActorRef): Unit = {
    customWebSocket(exchange, new DownstreamActor {
      override def onOpen(upstream: ActorRef): Unit = mediator ! RegisterMediatorSubscriber(upstream)

      override def onClose(upstream: ActorRef): Unit = mediator ! PoisonPill

      override def receiveMessage(upstream: ActorRef): PartialFunction[Message, Unit] = {
        case text: TextMessage =>
          val schemaFqn = text.getStrictText
          val (schemaId, _) = avroSerde.getRuntimeSchema(schemaFqn)
          require(schemaId >= 0, s"Could not determine runtime schema for $schemaFqn")
          upstream ! buildSchemaPushMessage(schemaId)
        case binary: BinaryMessage =>
          try {
            val buf = binary.getStrictData.asByteBuffer
            buf.get(0) match {
              case 123 => upstream ! buildSchemaPushMessage(schemaId = buf.getInt(1))
              case 0 => try {
                val record: Any = avroSerde.read(buf)
                mediator ! record
              } catch {
                case NonFatal(e) => log.error(e, "Invalid avro object received from the client")
              }
            }
          } catch {
            case NonFatal(e) => log.warning("Invalid websocket binary avro message", e)
          }

      }

      def buildSchemaPushMessage(schemaId: Int): ByteString = {
        try {
          val schemaBytes = avroSerde.getSchema(schemaId).toString(true).getBytes()
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

    }, new UpstreamActor {
      override def handle: Receive = {
        case direct: ByteString => push(BinaryMessage.Strict(direct)) //ByteString as the direct response from above downstream handler
        case None => push(BinaryMessage.Strict(ByteString())) //non-existent or delete key-value from the mediator
        case Some(value: AvroRecord) => push(BinaryMessage.Strict(ByteString(avroSerde.toBytes(value)))) //key-value from the mediator
        case value: AvroRecord => push(BinaryMessage.Strict(ByteString(avroSerde.toBytes(value)))) //key-value from the mediator
        case opt: Optional[_] if !opt.isPresent => push(BinaryMessage.Strict(ByteString()))
        case opt: Optional[_] if opt.get.isInstanceOf[AvroRecord] => push(BinaryMessage.Strict(ByteString(avroSerde.toBytes(opt.get))))
      }
    })
  }

  /**
    * customWebSocket:
    * |
    * |
    * |                                                      +----------+     onOpen(upstream) ...
    * |                                   +------+   send    |DOWNSTREAM+---> handleMessage(Message) ...
    * |                                   |      +---------> |ACTOR     |     onClose() ...
    * |                                   |      |   close   +-----+----+
    * |                                   | WEB  |                 |
    * |                       CLIENT <---->SOCKET|             push|close
    * |                                   |      |                 |
    * |                                   |      |   push    +-----v----+
    * |                                   |      <---------+ |UPSTREAM  |
    * |                                   +------+           |ACTOR     <---+ handle() ...
    * |                                                      +----------+
    * |
    *
    * @param exchange   web socket echange as captured in the http interface
    * @param downstream actor which will handle messges sent from the client
    * @param upstream   actor which will handle messages directed at the client
    */
  def customWebSocket(exchange: WebSocketExchange, downstream: => DownstreamActor, upstream: => UpstreamActor): Unit = {
    //implicit val materializer: ActorMaterializer = ActorMaterializer.create(context.system)
    val downstreamRef = context.actorOf(Props(downstream))
    val upstreamRef = context.actorOf(Props(upstream))
    implicit val timeout = Timeout(100 milliseconds)
    Await.result(downstreamRef ? RegisterMediatorSubscriber(upstreamRef), timeout.duration)

    val downMessageSink = Sink.actorRef[Message](downstreamRef, PoisonPill)
    val pushMessageSource = Source.fromPublisher(ActorPublisher[Message](upstreamRef))
    val flow = Flow.fromSinkAndSource(downMessageSink, pushMessageSource)

    val upgrade = exchange.upgrade.handleMessages(flow)
    exchange.response.success(upgrade)
  }

  implicit def textToMessage(text: String): Message = TextMessage.Strict(text)

  trait DownstreamActor extends Actor {

    private var upstream: ActorRef = null

    def onOpen(upstream: ActorRef): Unit = ()

    def onClose(upstream: ActorRef): Unit = ()

    def receiveMessage(upstream: ActorRef): PartialFunction[Message, Unit]

    def receiveHandleError(upstream: ActorRef): PartialFunction[Throwable, Unit] = PartialFunction.empty

    override def postStop(): Unit = {
      log.debug("WebSocket Downstream Actor Closing")
      if (upstream != null) {
        upstream ! PoisonPill
        onClose(upstream)
      }
    }

    final override def receive: Receive = {
      case RegisterMediatorSubscriber(upstream) =>
        this.upstream = upstream
        sender ! true
        onOpen(upstream)
      case msg: Message if upstream == null => log.warning(s"websocket actors not yet connected, dropping 1 message ${msg.getClass}")
      case msg: Message =>
        try {
          receiveMessage(upstream)(msg)
        } catch {
          case NonFatal(e) =>
            var logged = false
            val errorHandler: PartialFunction[Throwable, Unit] = receiveHandleError(upstream) orElse {
              case RequestException(status: StatusCode) => upstream ! Map("type" -> "error", "code" -> status.intValue, "message" -> status.defaultMessage)
              case e: ExecutionException => receiveHandleError(upstream)(e.getCause)
              case e: NoSuchElementException => upstream ! Map("type" -> "error", "code" -> 404, "message" -> e.getMessage())
              case e: IllegalArgumentException => upstream ! Map("type" -> "error", "code" -> 400, "message" -> e.getMessage())
              case e: IllegalStateException => upstream ! Map("type" -> "error", "code" -> 409, "message" -> e.getMessage())
              case _: IllegalAccessException => upstream ! Map("type" -> "error", "code" -> 403, "message" -> "Forbidden")
              case _: SecurityException => upstream ! Map("type" -> "error", "code" -> 401, "message" -> "Unauthorized")
              case _: NotImplementedError | _: UnsupportedOperationException => upstream ! Map("type" -> "error", "code" -> 501, "message" -> "Not Implemented")
              case NonFatal(e) =>
                logged = true
                log.error(e, "Json WebSocket receive handler failure")
                upstream ! Map("type" -> "error", "code" -> 500, "message" -> "Something went wrong with the server")
            }
            errorHandler(e)
            if (!logged) log.warning(e.getMessage)
        }
    }

  }

  trait UpstreamActor extends ActorPublisher[Message] with ActorHandler {
    val conf = Conf(context.system.settings.config).Affi

    final val maxBufferSize = conf.Node.Gateway.Http.MaxWebSocketQueueSize()

    override def postStop(): Unit = {
      log.debug("WebSocket Upstream Actor Closing")
    }

    private val buffer = new util.LinkedList[Message]()

    protected def push(messages: Message*) = {
      while (buffer.size + messages.size > maxBufferSize) {
        buffer.pop()
      }
      messages.foreach(buffer.add)
      while (isActive && totalDemand > 0 && buffer.size > 0) {
        onNext(buffer.pop)
      }
    }

    override def manage: Receive = {
      case Request(_) => push()
      case Cancel => log.warning("UpstreamActor received Cancel event")
    }

    override def unhandled: Receive = {
      case msg: Message => push(msg)
      case other: Any => log.warning(s"only Message types can be pushed, ignoring: $other")
    }
  }


}