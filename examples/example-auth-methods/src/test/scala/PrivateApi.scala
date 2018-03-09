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

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, HttpChallenge}
import akka.http.scaladsl.model.{HttpResponse, headers}
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.actor.GatewayHttp
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH, QUERY}
import io.amient.affinity.core.http.{Encoder, HttpExchange}
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.util.{TimeCryptoProof, TimeCryptoProofSHA256}
import org.codehaus.jackson.annotate.JsonIgnore

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.control.NonFatal

final case class ConfigEntry(description: String, @JsonIgnore salt: String) extends AvroRecord {
  @JsonIgnore val crypto = new TimeCryptoProofSHA256(salt)
}

trait PrivateApi extends GatewayHttp {

  import context.dispatcher

  private val settings: State[String, ConfigEntry] = global[String, ConfigEntry]("settings")

  abstract override def handle: Receive = super.handle orElse {

    case http@HTTP(GET, PATH("settings"), _, _) => AUTH_ADMIN(http) { (user: String) =>
      try {
        Future.successful {
          Encoder.json(OK, Map(
            "credentials" -> user,
            "settings" -> settings.iterator.map(record => (record.key, record.value)).toMap
          ))
        }
      } catch {
        case NonFatal(e) => Future.failed(e)
      }
    }


    case http@HTTP(POST, PATH("settings", "add"), QUERY(("key", key)), response) => AUTH_ADMIN(http) { (user: String) =>
      Future.successful {
        settings(key) match {
          case Some(existinKey) => Encoder.json(BadRequest, "That key already exists" -> key)
          case None =>
            val salt = TimeCryptoProof.toHex(TimeCryptoProof.generateSalt())
            settings.replace(key, ConfigEntry(key, salt))
            Encoder.json(OK, salt)
        }
      }
    }

    case http@HTTP(GET, PATH("status"), _, response) => AUTH_ADMIN(http)(user => Future.successful {
      Encoder.json(OK, Map(
        "keyspaces" -> describeKeyspaces
      ))
    })

  }

  /**
    * AUTH_ADMIN is a pattern match extractor that can be used in handlers that want to
    * use Basic HTTP Authentication for administrative tasks like creating new keys etc.
    */
  object AUTH_ADMIN {

    def apply(exchange: HttpExchange)(code: (String) => Future[HttpResponse]): Unit = {

      def executeCode(user: String): Future[HttpResponse] = try {
        code(user)
      } catch {
        case NonFatal(e) => Future.failed(e)
      }

      val auth = exchange.request.header[Authorization]
      val response = exchange.promise
      val credentials = for (Authorization(c@BasicHttpCredentials(username, password)) <- auth) yield c
      settings("admin") match {
        case None =>
          credentials match {
            case Some(BasicHttpCredentials(username, newAdminPassword)) if username == "admin" =>
              fulfillAndHandleErrors(response) {
                settings.replace("admin", ConfigEntry("Administrator Account", TimeCryptoProof.toHex(newAdminPassword.getBytes))) flatMap {
                  _ => executeCode(username)
                }
              }
            case _ => response.success(HttpResponse(
              Unauthorized, headers = List(headers.`WWW-Authenticate`(HttpChallenge("BASIC", Some("Create admin password"))))))
          }
        case Some(ConfigEntry(any, adminPassword)) => credentials match {
          case Some(BasicHttpCredentials(username, password)) if username == "admin"
            && TimeCryptoProof.toHex(password.getBytes) == adminPassword =>

            fulfillAndHandleErrors(response) {
              executeCode(username)
            }
          case _ =>
            response.success(HttpResponse(Unauthorized, headers = List(headers.`WWW-Authenticate`(HttpChallenge("BASIC", None)))))
        }
      }

    }

  }

}
