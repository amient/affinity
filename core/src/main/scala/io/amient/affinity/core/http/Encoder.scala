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

package io.amient.affinity.core.http

import java.io.{OutputStreamWriter, Writer}
import java.util.zip.GZIPOutputStream

import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.avro.util.ByteBufferOutputStream

import scala.collection.JavaConverters._
import scala.collection.mutable

object Encoder {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def json(status: StatusCode, value: Any, gzip: Boolean = true): HttpResponse = {
    //FIXME if value.isInstanceOf[AvroRecord[_]] then use to string instead of DefaultScalaModule
    val h = mutable.ListBuffer[HttpHeader]()
    h += headers.Date(DateTime.now)
    h += headers.`Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = json(value, gzip), headers = h.toList)
  }

  def json(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`application/json`, gzip) { writer =>
      mapper.writeValue(writer, value)
    }
  }

  def html(status: StatusCode, value: Any, gzip: Boolean = true): HttpResponse = {
    val h = mutable.ListBuffer[HttpHeader]()
    h += headers.Date(DateTime.now)
    h += headers.`Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = html(value, gzip), headers = h.toList)
  }

  def html(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`text/html(UTF-8)`, gzip) { writer =>
      writer.append(value.toString)
      writer.close()
    }
  }

  private def encode(contentType: ContentType, gzip: Boolean )(f: (Writer) => Unit): MessageEntity = {

    val out = new ByteBufferOutputStream()
    val writer = if (gzip) new OutputStreamWriter(new GZIPOutputStream(out)) else new OutputStreamWriter(out)
    f(writer)
    val buffers = out.getBufferList

    buffers.size match {
      case 1 => HttpEntity(contentType, ByteString(buffers.get(0)))
      case _ => HttpEntity(contentType, Source(buffers.asScala.map(ByteString(_)).toList))
    }
  }
}
