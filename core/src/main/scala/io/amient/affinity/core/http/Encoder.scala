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

import java.io.{OutputStream, OutputStreamWriter, Writer}
import java.util.zip.GZIPOutputStream

import akka.http.scaladsl.model.headers.HttpEncodings
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.avro.generic.{GenericDatumWriter, IndexedRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.util.ByteBufferOutputStream

import scala.collection.JavaConverters._
import scala.collection.mutable

object Encoder {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def json(status: StatusCode, value: Any, gzip: Boolean = true): HttpResponse = {
    val h = mutable.ListBuffer[HttpHeader]()
    h += headers.Date(DateTime.now)
    h += headers.`Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = json(value, gzip), headers = h.toList)
  }

  def json(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`application/json`, gzip) { (out, writer) =>
      value match {
        case record: IndexedRecord =>
          val schema = record.getSchema
          val avroEncoder = EncoderFactory.get().jsonEncoder(schema, out)
          val datumWriter = new GenericDatumWriter[Any](schema)
          datumWriter.write(record, avroEncoder)
          avroEncoder.flush()
          writer.close()
        case other => mapper.writeValue(writer, other)
      }
    }
  }

  def html(status: StatusCode, value: Any, gzip: Boolean = true): HttpResponse = {
    val h = mutable.ListBuffer[HttpHeader]()
    h += headers.Date(DateTime.now)
    h += headers.`Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = html(value, gzip), headers = h.toList)
  }

  def html(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`text/html(UTF-8)`, gzip) { (out, writer) =>
      writer.append(value.toString)
      writer.close()
    }
  }

  def plain(status: StatusCode, value: Any, gzip: Boolean = true): HttpResponse = {
    val h = mutable.ListBuffer[HttpHeader]()
    h += headers.Date(DateTime.now)
    h += headers.`Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = plain(value, gzip), headers = h.toList)
  }

  def plain(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`text/plain(UTF-8)`, gzip) { (out, writer) =>
      writer.append(value.toString)
      writer.close()
    }
  }

  private def encode(contentType: ContentType, gzip: Boolean )(f: (OutputStream, Writer) => Unit): MessageEntity = {

    val out = new ByteBufferOutputStream()
    val gout = if (gzip) new GZIPOutputStream(out) else out
    val writer = new OutputStreamWriter(gout)
    f(gout, writer)
    val buffers = out.getBufferList

    buffers.size match {
      case 1 => HttpEntity(contentType, ByteString(buffers.get(0)))
      case _ => HttpEntity(contentType, Source(buffers.asScala.map(ByteString(_)).toList))
    }
  }
}
