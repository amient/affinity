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

import java.security.cert.X509Certificate
import java.security.{KeyStore, SecureRandom}
import java.util
import java.util.concurrent.ConcurrentHashMap
import javax.net.ssl.{KeyManagerFactory, SSLContext, X509TrustManager}

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
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Cluster.{CheckClusterAvailability, ClusterAvailability}
import io.amient.affinity.core.actor.Controller.GracefulShutdown
import io.amient.affinity.core.cluster.Coordinator.MasterStatusUpdate
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.http.{Encoder, HttpExchange, HttpInterface}
import io.amient.affinity.core.serde.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.util.ByteUtils
import org.apache.avro.util.ByteBufferInputStream

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.language.postfixOps

object Gateway {
  final val CONFIG_HTTP_HOST = "affinity.node.gateway.http.host"
  final val CONFIG_HTTP_PORT = "affinity.node.gateway.http.port"

  final val CONFIG_TLS_KEYSTORE_PKCS = "affinity.node.gateway.tls.keystore.standard"
  final val CONFIG_TLS_KEYSTORE_PASSWORD = "affinity.node.gateway.tls.keystore.password"
  final val CONFIG_TLS_KEYSTORE_RESOURCE = "affinity.node.gateway.tls.keystore.resource"
  final val CONFIG_TLS_KEYSTORE_FILE = "affinity.node.gateway.tls.keystore.file"
}

abstract class Gateway extends Actor {

  import Gateway._

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

  private val services = new ConcurrentHashMap[Class[_ <: Actor], ActorRef]

  val cluster = context.actorOf(Props(new Cluster()), name = "cluster")

  import context.system

  val sslContext = if (!config.hasPath(CONFIG_TLS_KEYSTORE_PASSWORD)) None else Some(SSLContext.getInstance("TLS"))
  sslContext.foreach { context =>
    log.info("Configuring SSL Context")
    val password = config.getString(CONFIG_TLS_KEYSTORE_PASSWORD).toCharArray
    val ks = KeyStore.getInstance(config.getString(CONFIG_TLS_KEYSTORE_PKCS))
    if (config.hasPath(CONFIG_TLS_KEYSTORE_RESOURCE)) {
      ks.load(getClass.getClassLoader.getResourceAsStream(config.getString(CONFIG_TLS_KEYSTORE_RESOURCE)), password)
    } else {
      ks.load(getClass.getClassLoader.getResourceAsStream(config.getString(CONFIG_TLS_KEYSTORE_FILE)), password)
    }
    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)
    context.init(keyManagerFactory.getKeyManagers, null, new SecureRandom)
  }

  private val httpInterface: HttpInterface = new HttpInterface(
    config.getString(CONFIG_HTTP_HOST), config.getInt(CONFIG_HTTP_PORT), sslContext)


  def describeServices = services.asScala.map { case (k, v) => (k.toString, v.path.toString) }

  def describeRegions: List[String] = {
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
    case e: NoSuchElementException => e.printStackTrace(); HttpResponse(NotFound)
    case e: IllegalArgumentException => HttpResponse(BadRequest)
    case e: UnsupportedOperationException => HttpResponse(NotImplemented)
    case e: IllegalStateException => HttpResponse(ServiceUnavailable)
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

  private var handlingSuspended = true
  private val suspendedQueueMaxSize = 1000
  //TODO configurable suspended queue max size
  private val suspendedHttpRequestQueue = scala.collection.mutable.ListBuffer[HttpExchange]()

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

    case msg@MasterStatusUpdate("regions", add, remove) => sender.reply(msg) {
      remove.foreach(ref => cluster ! RemoveRoutee(ActorRefRoutee(ref)))
      add.foreach(ref => cluster ! AddRoutee(ActorRefRoutee(ref)))
      cluster ! CheckClusterAvailability()
    }

    case msg@MasterStatusUpdate("services", add, remove) => sender.reply(msg) {
      add.foreach(ref => services.put(Class.forName(ref.path.name).asSubclass(classOf[Actor]), ref))
      remove.foreach(ref => services.remove(Class.forName(ref.path.name).asSubclass(classOf[Actor]), ref))
      implicit val timeout = Timeout(1 second)
      cluster ! CheckClusterAvailability()
    }

    case msg@ClusterAvailability(suspended) if (suspended != handlingSuspended) =>
      handlingSuspended = suspended
      context.system.eventStream.publish(msg)
      if (!suspended) {
        log.info("Handling Resumed")
        val reprocess = suspendedHttpRequestQueue.toList
        suspendedHttpRequestQueue.clear
        if (reprocess.length > 0) log.warning(s"Re-processing ${reprocess.length} suspended http requests")
        reprocess.foreach(handle(_))
      } else {
        log.warning("Handling Suspended")
      }

    case exchange: HttpExchange if (handlingSuspended) =>
      log.warning("Hadnling suspended, enqueuing request: " + exchange.request)
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

  abstract override def handle: Receive = super.handle orElse {
    case http@HTTP(GET, PATH("affinity.js"), _, response) => response.success(Encoder.plain(OK, afjs))
  }

  protected def jsonWebSocket(upgrade: UpgradeToWebSocket, stateStoreName: String, key: Any)
                             (pfCustomHandle: PartialFunction[JsonNode, Unit]): Future[HttpResponse] = {
    genericWebSocket(upgrade, stateStoreName, key) {
      case text: TextMessage => log.info(text.getStrictText)
      case binary: BinaryMessage =>
        val buf = binary.getStrictData.asByteBuffer
        val json = Encoder.mapper.readValue(new ByteBufferInputStream(List(buf).asJava), classOf[JsonNode])
        pfCustomHandle(json)
    } {
      case None => TextMessage.Strict("null")
      case Some(value) => TextMessage.Strict(Encoder.json(value))
      case direct: ByteString => BinaryMessage.Strict(direct)
    }
  }

  protected def avroWebSocket(http: HttpExchange, stateStoreName: String, key: Any)
                             (pfCustomHandle: PartialFunction[Any, Unit]): Unit = {
    http.request.header[UpgradeToWebSocket] match {
      case None => Future.failed(new IllegalArgumentException("WebSocket connection required"))
      case Some(upgrade) =>
        import context.dispatcher
        fulfillAndHandleErrors(http.promise) {
          avroWebSocket(upgrade, stateStoreName, key)(pfCustomHandle)
        }
    }
  }

  protected def avroWebSocket(upgrade: UpgradeToWebSocket, stateStoreName: String, key: Any)
                             (pfCustomHandle: PartialFunction[Any, Unit]): Future[HttpResponse] = {
    if (!serializers.contains(101)) throw new IllegalArgumentException("No AvroSerde is registered")
    val avroSerde = serializers(101).asInstanceOf[AvroSerde]

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

    genericWebSocket(upgrade, stateStoreName, key) {
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
                case forwardToBackend: AvroRecord[_] => cluster ! forwardToBackend
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

  protected def genericWebSocket(upgrade: UpgradeToWebSocket, stateStoreName: String, key: Any)
                                (pfDown: PartialFunction[Message, Any])
                                (pfPush: PartialFunction[Any, Message]): Future[HttpResponse] = {
    import context.dispatcher
    implicit val scheduler = context.system.scheduler
    implicit val materializer = ActorMaterializer.create(context.system)
    implicit val timeout = Timeout(1 second)

    cluster ? (key, Partition.INTERNAL_CREATE_KEY_VALUE_MEDIATOR, stateStoreName) map {
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
              case response: ByteString => frontend.foreach(_ ! response)
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

          private val buffer = new util.LinkedList[Message]()

          //TODO max buffer size
          private def doPush(messages: Message*) = {
            messages.foreach(buffer.add)
            while (isActive && totalDemand > 0 && buffer.size > 0) {
              sender ! onNext(buffer.pop)
            }
          }

          override def receive: Receive = {
            case Terminated(source) => context.stop(self)
            case Request(_) => doPush()
            case pushMessage => doPush(pfPush(pushMessage))
          }
        }))
        val flow = Flow.fromSinkAndSource(downMessageSink, pushMessageSource)
        upgrade.handleMessages(flow)
    }
  }

}