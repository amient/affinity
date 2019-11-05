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

import java.util.Optional
import java.util.concurrent.ExecutionException

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{CacheDirectives, `Cache-Control`}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.stream._
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import com.typesafe.config.Config
import io.amient.affinity.avro.record.{AvroRecord, AvroSerde}
import io.amient.affinity.core.actor.Controller.CreateGateway
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.http._
import io.amient.affinity.core.storage.Record
import io.amient.affinity.core.util.ByteUtils

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

import scala.util.control.NonFatal

trait GatewayHttp extends Gateway {

  private val log = Logging.getLogger(context.system, this)

  import context.system

  val rejectSuspendedHttpRequests: Boolean = conf.Affi.Node.Gateway.RejectSuspendedHttpRequests()

  def listenerConfigs: Seq[HttpInterfaceConf] = conf.Affi.Node.Gateway.Listeners().asScala

  val interfaces: List[HttpInterface] = listenerConfigs.map(new HttpInterface(_)).toList

  private val onlineCounter = metrics.counter("http.gateway." + interfaces.head.port)

  private implicit val executor = scala.concurrent.ExecutionContext.Implicits.global

  abstract override def preStart(): Unit = {
    super.preStart()
    require(interfaces.size >= 1, "At least one interface must be defined for Http Gateway to function")
  }

  abstract override def postStop(): Unit = try {
    interfaces.foreach(_.close)
    log.info("Http Interface closed")
    Http().shutdownAllConnectionPools()
  } finally {
    super.postStop()
  }

  abstract override def onSuspend(): Unit = try {
    log.info("Http Gateway Suspended")
    onlineCounter.dec()
  } finally {
    super.onSuspend
  }

  abstract override def onResume(): Unit = try {
    log.info("Http Gateway Resumed")
    onlineCounter.inc()
  } finally {
    super.onResume
  }

  abstract override def manage: Receive = super.manage orElse {
    case CreateGateway =>
      log.info("Gateway starting")
      val ports = interfaces.map(_.bind(self).getPort)
      context.parent ! Controller.GatewayCreated(ports)
  }

  override def onHold(message: Any, sender: ActorRef): Unit = message match {
    case exchange: HttpExchange if rejectSuspendedHttpRequests =>
      exchange.promise.success(HttpResponse(StatusCodes.ServiceUnavailable))
    case _ => super.onHold(message, sender)
  }

  override def unhandled: Receive = {
    case exchange: HttpExchange => {
      //no handler matched the HttpExchange
      exchange.promise.success(HttpResponse(NotFound, entity = "Server did not understand your request"))
    }
    case _ => throw new IllegalArgumentException(s"GatewayHttp can only handle HttpExchange messages")
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
      case None => response.success(HttpResponse(NotFound))
      case _ => response.success(HttpResponse(Accepted))
    }
  } catch {
    case e: Throwable => response.success(handleException(headers)(e))
  }

  def handleAsText(response: Promise[HttpResponse], dataGenerator: => Any): Unit = {
    handleAsText(response, dataGenerator, gzip = true, headers = List())
  }

  def handleAsText(response: Promise[HttpResponse], dataGenerator: => Any, headers: List[HttpHeader]): Unit = {
    handleAsText(response, dataGenerator, gzip = true, headers)
  }

  def handleAsText(response: Promise[HttpResponse], dataGenerator: => Any, gzip: Boolean, headers: List[HttpHeader]): Unit = {
    handleAs(response, dataGenerator, headers)(data => Encoder.text(OK, data, gzip))
  }

  def handleAsJson(response: Promise[HttpResponse], dataGenerator: => Any): Unit = {
    handleAsJson(response, dataGenerator, gzip = true, headers = List())
  }

  def handleAsJson(response: Promise[HttpResponse], dataGenerator: => Any, headers: List[HttpHeader]): Unit = {
    handleAsJson(response, dataGenerator, gzip = true, headers)
  }

  def handleAsJson(response: Promise[HttpResponse], dataGenerator: => Any, gzip: Boolean, headers: List[HttpHeader]): Unit = {
    handleAs(response, dataGenerator, headers)(data => Encoder.json(OK, data, gzip))
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


  def handleWith(promise: Promise[HttpResponse], headers: List[HttpHeader] = List())(f: => Future[HttpResponse])(implicit ctx: ExecutionContext): Unit = {
    promise.completeWith(try {
      f.recover(handleException(headers))(ctx)
    } catch {
      case t: Throwable => Future.successful(handleException(headers)(t))
    })
  }


  def handleException: PartialFunction[Throwable, HttpResponse] = handleException(List())

  def handleException(headers: List[HttpHeader]): PartialFunction[Throwable, HttpResponse] = {
    case RequestException(status, serverMessage) =>
      log.error(s"${status.intValue} ${status.reason} - $serverMessage")
      val validHttpStatusCode = status.intValue().toString.take(3).toInt
      val validHttpStatus = StatusCode.int2StatusCode(validHttpStatusCode)
      val message = status.intValue.toString + " " + status.reason
      HttpResponse(validHttpStatus, entity = message, headers = headers)
    case e: ExecutionException => handleException(headers)(e.getCause)
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
    val secureHeaders = headers :+ `Cache-Control`(CacheDirectives.`no-store`, CacheDirectives.`must-revalidate`)
      status.defaultMessage() match {
      case null => HttpResponse(status, headers = secureHeaders)
      case message => HttpResponse(status, entity = message, headers = secureHeaders)
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

  private val avroSerde = AvroSerde.create(config)

  private implicit val materializer: ActorMaterializer = ActorMaterializer.create(context.system)

  abstract override def handle: Receive = super.handle orElse {
    case HTTP(GET, PATH("affinity.js"), _, response) if afjs.isDefined => response.success(Encoder.text(OK, afjs.get, gzip = true))
  }


  /**
    * jsonWebSocket:
    * +                                        +---------------+
    * |                                        |  GraphStage   |
    * |                           send:TEXT    |               |  send:VALUE
    * |                 +------+  send:BIN    +-+ receive(JSON)+---------------+
    * |                 |      +------------> |-|              |  close   +-----------------------------+
    * |                 |      |    close     +-+              |          |    |                        |
    * |                 | WEB  |               |               |          | +--v------+     +---------+ |
    * |     CLIENT <---->SOCKET|               |               |          | |MEDIATOR +----->KEY|VALUE| |
    * |                 |      |               |               |          | +--+------+     +---------+ |
    * |                 |      |     push     +-+              |          |    |  STATE PARTITION       |
    * |                 |      | <--------------| send()       |   push   +-----------------------------+
    * |                 +------+     TEXT     +-+ +--------+ <-----------------+
    * |                                        |  | ...... |   |   JSON
    * |                                        |  +--------+   |
    * +                                        +---------------+
    *
    * @param exchange web socket echange as captured in the http interface
    * @param mediator key-value mediator create using connectKeyValueMediator()
    */
  def jsonWebSocket(exchange: WebSocketExchange, mediator: ActorRef): Unit = {
    webSocket(exchange, new WSHandler {
      val subscriber = context.actorOf(Props(new Actor {

        override def receive: Receive = {
          case rec: Record[_, _] if rec.value == null => send(TextMessage.Strict("{}"))
          case rec: Record[_, _] => send(TextMessage.Strict(Encoder.json(rec.value)))
          case None => send(TextMessage.Strict("{}"))
          case Some(value) => send(TextMessage.Strict(Encoder.json(value)))
          case opt: Optional[_] if !opt.isPresent => send(TextMessage.Strict("{}"))
          case opt: Optional[_] => send(TextMessage.Strict(Encoder.json(opt.get)))
          case other => send(TextMessage.Strict(Encoder.json(other)))
        }
      }))

      override def onOpen() : Unit = mediator ! RegisterMediatorSubscriber(subscriber)

      override def onClose(): Unit = mediator ! PoisonPill

      override def receive(): PartialFunction[Message, Unit] = {
        case text: TextMessage => mediator ! text.getStrictText
        case binary: BinaryMessage => mediator ! Decoder.text(binary.dataStream)
      }
    })
  }

  /**
    * avroWebSocket:
    *
    * +                                        +---------------+
    * |                                        |  GraphStage   |
    * |                           send:TEXT    |               |  send:VALUE
    * |                 +------+  send:BIN    +-+ receiee(AVRO)+---------------+
    * |                 |      +------------> |-|   +          |  close   +-----------------------------+
    * |                 |      |    close     +-+   |          |          |    |                        |
    * |                 | WEB  |               |    |/Schema   |          | +--v------+     +---------+ |
    * |     CLIENT <---->SOCKET|               |    |/Close    |          | |MEDIATOR +----->KEY|VALUE| |
    * |                 |      |               |    |          |          | +--+------+     +---------+ |
    * |                 |      |     push     +-+   v          |          |    |  STATE PARTITION       |
    * |                 |      | <--------------| send(AVRO)   |   push   +-----------------------------+
    * |                 +------+     TEXT     +-+ +--------+ <-----------------+
    * |                                        |  | ...... |   |   VALUE
    * |                                        |  +--------+   |
    * +                                        +---------------+
    *
    *
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
    webSocket(exchange, new WSHandler {
      val subscriber = context.actorOf(Props(new Actor {
        override def receive: Receive = {
          case None => send(BinaryMessage.Strict(ByteString())) //non-existent or delete key-value from the mediator
          case Some(value: AvroRecord) => send(BinaryMessage.Strict(ByteString(avroSerde.toBytes(value)))) //key-value from the mediator
          case rec: Record[_, _] if rec.value == null => send(BinaryMessage.Strict(ByteString())) //replication record
          case rec: Record[_, _] => send(BinaryMessage.Strict(ByteString(avroSerde.toBytes(rec.value)))) //replication record
          case value: AvroRecord => send(BinaryMessage.Strict(ByteString(avroSerde.toBytes(value)))) //key-value from the mediator
          case opt: Optional[_] if !opt.isPresent => send(BinaryMessage.Strict(ByteString()))
          case opt: Optional[_] if opt.get.isInstanceOf[AvroRecord] => send(BinaryMessage.Strict(ByteString(avroSerde.toBytes(opt.get))))
        }
      }))

      override def onOpen() : Unit = mediator ! RegisterMediatorSubscriber(subscriber)

      override def onClose(): Unit = mediator ! PoisonPill

      override def receive(): PartialFunction[Message, Unit] = {
        case text: TextMessage =>
          val schemaFqn = text.getStrictText
          val (schemaId, _) = avroSerde.getRuntimeSchema(schemaFqn)
          require(schemaId >= 0, s"Could not determine runtime schema for $schemaFqn")
          send(BinaryMessage.Strict(buildSchemaPushMessage(schemaId)))
        case binary: BinaryMessage =>
          try {
            val buf = binary.getStrictData.asByteBuffer
            buf.get(0) match {
              case 123 => send(BinaryMessage.Strict(buildSchemaPushMessage(schemaId = buf.getInt(1))))
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
    })
  }

  /**
    * webSocket:
    * +                                                             WSHandler
    * |                                                      +----------------------+
    * |                                                      |           onOpen() +----+
    * |                                                      |GraphStage            |
    * |                                                      +---------+ receive()+----+
    * |                                   +------+   send    ||        |            |
    * |                                   |      +---------> || +---+  |            |
    * |                                   |      |   close   ||     |  |            |
    * |                                   | WEB  |           ||     |  |            |
    * |                       CLIENT <---->SOCKET|           ||     |  |            |
    * |                                   |      |           ||     |  |            |
    * |                                   |      |   push    ||     |  |            |
    * |                                   |      <---------+ || <---+  |            |
    * |                                   +------+           ||        |  send() +-----+
    * |                                                      +---------+            |
    * +                                                      |            onClose()+---+
    *                                                        +----------------------+
    *
    * @param exchange   web socket ecchange as captured in the http interface
    */
  def webSocket(exchange: WebSocketExchange, handler: => WSHandler): Unit = {
    val maxBufferSize = conf.Affi.Node.Gateway.MaxWebSocketQueueSize()
    val graph = new WSFlowStage(maxBufferSize, handler)
    val upgrade = exchange.upgrade.handleMessages(Flow.fromGraph(graph))
    exchange.response.success(upgrade)
  }

}

class WSFlowStage(maxBufferSize: Int, mat: => WSHandler) extends GraphStage[FlowShape[Message, Message]] {

  val in = Inlet[Message]("WS.from.client")
  val out = Outlet[Message]("WS.to.client")

  override val shape = FlowShape(in, out)

  //TODO investigate degradation in latency after replacing ActorPublisher with GraphStage
  override def createLogic(attr: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {
      val clientBuffer = scala.collection.mutable.Queue[Message]()
      var clientWaitingForData = false

      def isFull = clientBuffer.size == maxBufferSize

      val handler: WSHandler = mat
      handler.apply(sendReply)

      def sendReply(msg: Message): Unit = {
        clientBuffer.enqueue(msg)
        if (clientWaitingForData) {
          clientWaitingForData = false
          push(out, clientBuffer.dequeue())
        }
      }

      override def preStart(): Unit = {
        pull(in)
        handler.onOpen()
      }

      override def postStop(): Unit = {
        handler.onClose()
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val msgFromClient: Message = grab(in)
          handler.receive(msgFromClient)
          if (!isFull) {
            pull(in)
          }
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (clientBuffer.isEmpty) {
            clientWaitingForData = true
          } else {
            val elem: Message = clientBuffer.dequeue
            push(out, elem)
          }
        }
      })


    }
  }
}


trait WSHandler {

  @volatile private[this] var replier: Message => Unit = null

  def apply(replier: Message => Unit) = this.replier = replier

  def onOpen(): Unit = ()

  def onClose(): Unit = ()

  final def send(msg: Message): Unit = replier(msg)

  def receive: PartialFunction[Message, Unit]

  //FIXME dow we need ?  def receiveHandleError(upstream: ActorRef): PartialFunction[Throwable, Unit] = PartialFunction.empty
}
