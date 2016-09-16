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

package io.amient.affinity.example.rest

import java.io.StringWriter

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri._
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, HttpChallenge}
import akka.http.scaladsl.model.{HttpEntity, _}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.{ActorState, Gateway}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.data.StringSerde
import io.amient.affinity.core.http.HttpExchange
import io.amient.affinity.core.storage.{KafkaStorage, MemStoreConcurrentMap}
import io.amient.affinity.example.data.{ConfigEntry, MyAvroSerde}
import io.amient.affinity.example.rest.handler._
import io.amient.util.TimeCryptoProof

import scala.concurrent.Promise
import scala.util.control.NonFatal

object HttpGateway {

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Gateway Node requires 1 argument: <http-port>")
    val httpPort = args(0).toInt
    val config = ConfigFactory.load("example").withValue(Gateway.CONFIG_HTTP_PORT, ConfigValueFactory.fromAnyRef(httpPort))

    new Node(config) {
      startGateway(new HttpGateway(config)
        with Describe
        with Ping
        with Fail
        with Connect
        with Access
      )
    }
  }
}


class HttpGateway(config: Config) extends Gateway(config) with ActorState {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
    * settings is a broadcast memstore which holds an example set of api keys for custom authentication
    * unlike partitioned mem stores all nodes see the same settings because they are linked to the same
    * partition 0. MemStoreConcurrentMap is mixed in instead of MemStoreSimpleMap because the settings
    * can be modified by other nodes and need to be accessed concurrently
    */
  val settings = state {
    new KafkaStorage[String, ConfigEntry](topic = "settings", 0, classOf[StringSerde], classOf[MyAvroSerde])
      with MemStoreConcurrentMap[String, ConfigEntry]
  }

  override def handleException: PartialFunction[Throwable, HttpResponse] = {
    case e: IllegalAccessError => errorValue(Forbidden, "Forbidden - " + e.getMessage)
    //errorValue(Unauthorized, "Unauthorized")
    case e: NoSuchElementException => errorValue(NotFound, "Haven't got that")
    case e: IllegalArgumentException => errorValue(BadRequest, "BadRequest - " + e.getMessage)
    case e: NotImplementedError => e.printStackTrace(); errorValue(NotImplemented, "Eeek! We have a bug..")
    case NonFatal(e) => e.printStackTrace(); errorValue(InternalServerError, "Well, something went wrong but we should be back..")
    case e => e.printStackTrace(); errorValue(ServiceUnavailable, "Something is seriously wrong with our servers..")
  }

  /**
    * AUTH_CRYPTO can be applied to requests that have been matched in the handler Receive method.
    * It is a partial function with curried closure that is invoked on succesful authentication with
    * the server signature that needs to be returned as part of response.
    * This is an encryption-based authentication using the keys and salts stored in the settings state.
    */
  object AUTH_CRYPTO {

    def apply(path: Path, query: Query, response: Promise[HttpResponse])(code: (String) => HttpResponse): Unit = {
      try {
        query.get("signature") match {
          case None => throw new IllegalAccessError
          case Some(sig) =>
            sig.split(":") match {
              case Array(k, clientSignature) => settings.get(k) match {
                case Some(configEntry) =>
                  if (configEntry.crypto.verify(clientSignature, path.toString)) {
                    val serverSignature = configEntry.crypto.sign(clientSignature)
                    response.success(code(serverSignature))
                  } else {
                    throw new IllegalAccessError(configEntry.crypto.sign(path.toString))
                  }
                case _ => throw new IllegalAccessError
              }
              case _ => throw new IllegalAccessError
            }
        }
      } catch {
        case e: IllegalAccessError => response.success(errorValue(Unauthorized, "Unauthorized, expecting " + e.getMessage))
        case e: Throwable => response.success(handleException(e))
      }
    }
  }

  /**
    * AUTH_ADMIN is a pattern match extractor that can be used in handlers that want to
    * use Basic HTTP Authentication for administrative tasks like creating new keys etc.
    */
  object AUTH_ADMIN {

    val adminConfig = settings.get("admin")

    def apply(exchange: HttpExchange)(code: (String) => HttpResponse): Unit = {
      val auth = exchange.request.header[Authorization]
      val response = exchange.promise
      val credentials = for (Authorization(c@BasicHttpCredentials(username, password)) <- auth) yield c
      adminConfig match {
        case None =>
          credentials match {
            case Some(BasicHttpCredentials(username, newAdminPassword)) if username == "admin" =>
              settings.put("admin", Some(ConfigEntry("Administrator Account", TimeCryptoProof.toHex(newAdminPassword.getBytes))))
              response.success(code(username))
            case _ => response.success(HttpResponse(
              Unauthorized, headers = List(headers.`WWW-Authenticate`(HttpChallenge("BASIC", Some("Create admin password"))))))
          }
        case Some(ConfigEntry(any, adminPassword)) => credentials match {
          case Some(BasicHttpCredentials(username, password)) if username == "admin"
            && TimeCryptoProof.toHex(password.getBytes) == adminPassword => response.success(code(username))
          case _ =>
            response.success(HttpResponse(Unauthorized, headers = List(headers.`WWW-Authenticate`(HttpChallenge("BASIC", None)))))
        }
      }

    }

  }

  /* Following are various helpers for HTTP <> AKKA transformations used in the handlers.
   * - all handlers are traits that extend HttpGateway.
   */

  def textValue(status: StatusCode, message: String): HttpResponse = {
    HttpResponse(status, entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, message))
  }

  def htmlValue(status: StatusCode, message: String): HttpResponse = {
    val formattedMessage = message.replace("\n", "<br/>")
    HttpResponse(status,
      entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h3>$formattedMessage</h3s>"))
  }

  def jsonValue(status: StatusCode, value: Any): HttpResponse = {
    val out = new StringWriter
    mapper.writeValue(out, value)
    val json = out.toString
    HttpResponse(status, entity = HttpEntity(ContentTypes.`application/json`, json))
  }

  def redirect(status: StatusCode, uri: Uri): HttpResponse = {
    HttpResponse(status, headers = List(headers.Location(uri)))
  }

  def errorValue(errorStatus: StatusCode, message: String, ct: ContentType = ContentTypes.`application/json`): HttpResponse = {
    ct match {
      case ContentTypes.`text/html(UTF-8)` => htmlValue(errorStatus, s"<h1> $message</h1>")
      case ContentTypes.`application/json` => jsonValue(errorStatus, Map("error" -> message))
      case _ => textValue(errorStatus, "error: " + message)
    }
  }

}