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
import java.util.Properties

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri._
import akka.http.scaladsl.model.{HttpEntity, _}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.amient.affinity.core.actor.Gateway
import io.amient.affinity.core.storage.{KafkaStorage, MemStoreConcurrentMap}
import io.amient.affinity.example.data.{AvroSerde, ConfigEntry}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

class HttpGateway(appConfig: Properties) extends Gateway(appConfig) {

  //broadcast memstore
//  val settings = new KafkaStorage[String, ConfigEntry](topic = "settings", 0)
//    with MemStoreConcurrentMap[String, ConfigEntry] {
//    val serde = new AvroSerde()
//
//    override def serialize: (String, ConfigEntry) => (Array[Byte], Array[Byte]) = (k, v) => {
//      (if (k == null) null else k.getBytes(), serde.toBinary(v))
//    }
//
//    override def deserialize: (Array[Byte], Array[Byte]) => (String, ConfigEntry) = (k, v) => {
//      (if (k == null) null else new String(k), serde.fromBytes(v, classOf[ConfigEntry]))
//    }
//  }
  //TODO provide a way for broadcasts to keep consuming new messages
//  settings.boot()
//  settings.tail()
  //settings.put("key1", Some(ConfigEntry("Some Key 1", "565BFA18808821339115A00FA61976B9")))

//  object AUTH {
//
//    def unapply(query: Query): Option[(Query, String)] = {
//      query.get("signature") match {
//        case None => None
//        case Some(sig) => {
//          sig.split(":") match {
//            case Array(k, clientSignature) => settings.get(k) match {
//              case None => None
//              case Some(configEntry) =>
//                //                val arg = request.uri.path.toString
//                //                if (configEntry.crypto.sign(arg) != clientSignature) {
//                //                  exchange.status = Some(Unauthorized)
//                //                  None
//                //                } else {
//                val signature = configEntry.crypto.sign(clientSignature)
//                Some(query, signature)
//              //                }
//            }
//            case _ => None
//          }
//        }
//      }
//    }
//  }

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

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
    val json = out.toString()
    HttpResponse(status, entity = HttpEntity(ContentTypes.`application/json`, json))
  }

  def errorValue(errorStatus: StatusCode, ct: ContentType, message: String): HttpResponse = {
    ct match {
      case ContentTypes.`text/html(UTF-8)` => htmlValue(errorStatus, s"<h1> $message</h1>")
      case ContentTypes.`application/json` => jsonValue(errorStatus, Map("error" -> message))
      case _ => textValue(errorStatus, "error: " + message)
    }
  }

  def redirect(status: StatusCode, uri: Uri): HttpResponse = {
    HttpResponse(status, headers = List(headers.Location(uri)))
  }


  def fulfillAndHandleErrors(promise: Promise[HttpResponse], future: Future[Any], ct: ContentType)
                            (f: Any => HttpResponse)(implicit ctx: ExecutionContext) {
    promise.completeWith(future map (f) recover {
      case e: NoSuchElementException => handleError(StatusCodes.NotFound)
      case e: IllegalArgumentException => e.printStackTrace(); handleError(StatusCodes.NotImplemented)
      case NonFatal(e) => e.printStackTrace(); handleError(StatusCodes.InternalServerError)
      case e => e.printStackTrace(); handleError(ServiceUnavailable)

    })
  }

  override def handleError(status: StatusCode): HttpResponse = handleError(status, ContentTypes.`application/json`)

  def handleError(status: StatusCode, ct: ContentType = ContentTypes.`application/json`): HttpResponse = {
    status match {
      case Forbidden => errorValue(Forbidden, ct, "Forbidden")
      case Unauthorized => errorValue(Unauthorized, ct, "Unauthorized")
      case NotFound => errorValue(NotFound, ct, "Haven't got that")
      case NotImplemented => errorValue(NotImplemented, ct, "Eeek! We have a bug..")
      case ServiceUnavailable => errorValue(ServiceUnavailable, ct, "Something is seriously wrong with our servers..")
      case _ => errorValue(InternalServerError, ct, "Well, something went wrong but we should be back..")
    }
  }

}