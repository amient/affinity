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

package io.amient.affinity.example.http.handler

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.ws.{BinaryMessage, TextMessage, UpgradeToWebSocket}
import akka.serialization.SerializationExtension
import akka.util.ByteString
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.core.serde.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.util.ByteUtils
import io.amient.affinity.example.rest.HttpGateway

trait WebSock extends HttpGateway {

  val afjs = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/affinity.js")).mkString
  val html = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/wsclient.html")).mkString
  val serde = SerializationExtension(context.system).serializerFor(classOf[AvroRecord[_]]).asInstanceOf[AvroSerde]

  import context.dispatcher

  abstract override def handle: Receive = super.handle orElse {

    //TODO move affinity.js into the core module
    case http@HTTP(GET, PATH("affinity.js"), _, response) => response.success(Encoder.plain(OK, afjs))

    case http@HTTP(GET, PATH("vertex"), QUERY(("id", INT(vertex))), response) =>

      http.request.header[UpgradeToWebSocket] match {
        case None => response.success(Encoder.html(OK, html))
        case Some(upgrade) => fulfillAndHandleErrors(http.promise) {
          keyValueWebSocket(upgrade, "graph", vertex) {
            //push/upstream push mapping
            case raw: Array[Byte] => BinaryMessage.Strict(ByteString(raw))
            //TODO move mapping of avro messages along with serde refernces to the abstract
            case Some(value: AvroRecord[_]) => BinaryMessage.Strict(ByteString(serde.toBytes(value)))
            case Some(value) => TextMessage.Strict(Encoder.json(value))
            case None => TextMessage.Strict("null")
          } {
            //downstream event mapping
            case text: TextMessage => println(text)
            //TODO move mapping of binnary messages along with serde refernces to the abstract
            case binary: BinaryMessage =>
              val buf = binary.getStrictData.asByteBuffer
              buf.get() match {
                case 0 => //TODO process avro message
                case 123 => //process avro schema request
                  val schemaId = buf.getInt()
                  val schemaBytes = serde.schema(schemaId).get._2.toString(true).getBytes()
                  val echoBytes = new Array[Byte](schemaBytes.length + 5)
                  echoBytes(0) = 123
                  ByteUtils.putIntValue(schemaId, echoBytes, 1)
                  Array.copy(schemaBytes, 0, echoBytes, 5, schemaBytes.length)
                  echoBytes
              }
          }
        }
      }

  }

}

