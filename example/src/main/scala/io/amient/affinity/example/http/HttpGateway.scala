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

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri._
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, HttpChallenge}
import akka.http.scaladsl.model.{headers, _}
import io.amient.affinity.example.{ConfigEntry, MyAvroSerde}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.{ActorState, Gateway}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.{HttpExchange, ResponseBuilder}
import io.amient.affinity.core.serde.primitive.StringSerde
import io.amient.affinity.core.storage.MemStoreConcurrentMap
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.TimeCryptoProof
import io.amient.affinity.example.rest.handler._

import scala.concurrent.Promise
import scala.util.control.NonFatal

object HttpGateway {

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Gateway Node requires 1 argument: <http-port>")
    val httpPort = args(0).toInt
    val config = ConfigFactory.load("example").withValue(Gateway.CONFIG_HTTP_PORT, ConfigValueFactory.fromAnyRef(httpPort))

    new Node(config) {
      startGateway(new HttpGateway
        with Describe
        with Ping
        with Fail
        with Connect
        with Access
      )
    }
  }
}


class HttpGateway extends Gateway with ActorState {

  /**
    * settings is a broadcast memstore which holds an example set of api keys for custom authentication
    * unlike partitioned mem stores all nodes see the same settings because they are linked to the same
    * partition 0. MemStoreConcurrentMap is mixed in instead of MemStoreSimpleMap because the settings
    * can be modified by other nodes and need to be accessed concurrently
    */
  val settings = storage {
    new KafkaStorage[String, ConfigEntry](brokers = "localhost:9092", topic = "settings", 0, classOf[StringSerde], classOf[MyAvroSerde])
      with MemStoreConcurrentMap[String, ConfigEntry]
  }

  override def handleException: PartialFunction[Throwable, HttpResponse] = {
    case e: IllegalAccessError => ResponseBuilder.json(Forbidden, "Forbidden" -> e.getMessage)
    //errorValue(Unauthorized, "Unauthorized")
    case e: NoSuchElementException => ResponseBuilder.json(NotFound, "Haven't got that" -> e.getMessage)
    case e: IllegalArgumentException => ResponseBuilder.json(BadRequest, "BadRequest" -> e.getMessage)
    case e: NotImplementedError =>
      e.printStackTrace()
      ResponseBuilder.json(NotImplemented, "Eeek! We have a bug..")
    case NonFatal(e) =>
      e.printStackTrace()
      ResponseBuilder.json(InternalServerError, "Well, something went wrong but we should be back..")
    case e =>
      e.printStackTrace()
      ResponseBuilder.json(ServiceUnavailable, "Something is seriously wrong with our servers..")
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
        case e: IllegalAccessError => response.success(ResponseBuilder.json(Unauthorized, "Unauthorized, expecting " -> e.getMessage))
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

}