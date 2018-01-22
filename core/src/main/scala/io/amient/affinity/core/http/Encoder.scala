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

import java.io.{ByteArrayOutputStream, OutputStreamWriter, Writer}
import java.util.zip.GZIPOutputStream

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Date, HttpEncodings, `Content-Encoding`}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.amient.affinity.avro.record.AvroJsonConverter
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.ByteBufferOutputStream
import org.codehaus.jackson.JsonNode

import scala.collection.JavaConverters._
import scala.collection.mutable

object Encoder {

  def json(value: Any): String = {
    val out = new ByteArrayOutputStream()
    val writer = new OutputStreamWriter(out)
    jsonWrite(value, writer)
    val result = out.toString("UTF-8")
    writer.close()
    result
  }

  def json(status: StatusCode, value: Any, gzip: Boolean = true, headers: List[HttpHeader] = List()): HttpResponse = {
    val h = mutable.ListBuffer[HttpHeader]()
    h ++= headers
    h += Date(DateTime.now)
    h += `Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = json(value, gzip), headers = h.toList)
  }

  def json(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`application/json`, gzip) { (writer) =>
      jsonWrite(value, writer)
      writer.close()
    }
  }

  private def jsonWrite(value: Any, writer: Writer): Unit = {

    def jsonWriteRecursive(value: Any, writer: Writer): Unit = {
      value match {
        case j: JsonNode => writer.append(j.toString())
        case null => writer.append("null")
        case b: Boolean => writer.append(b.toString)
        case b: Byte => writer.append(b.toString)
        case i: Int => writer.append(i.toString)
        case l: Long => writer.append(l.toString)
        case f: Float => writer.append(f.toString)
        case d: Double => writer.append(d.toString)
        case s: String => writer.append("\"").append(s.replace("\"", "\\\"")).append("\"")
        case record: IndexedRecord =>
          val schema = record.getSchema
          writer.append("{\"type\":\"" + schema.getFullName+ "\",\"data\":")
          writer.flush
          AvroJsonConverter.toJson(writer, schema, value)
          writer.append("}")
          writer.flush

        case None => jsonWriteRecursive(null, writer)
        case Some(optional) => jsonWriteRecursive(optional, writer)
        case i: Iterable[_] if (i.size > 0 && i.head.isInstanceOf[(_, _)] && i.head.asInstanceOf[(_, _)]._1.isInstanceOf[String]) =>
          writer.append("{")
          writer.flush()
          var first = true
          i.foreach { case (k, v) =>
            if (first) first = false
            else {
              writer.append(",")
              writer.flush()
            }
            jsonWriteRecursive(k, writer)
            writer.append(":")
            writer.flush()
            jsonWriteRecursive(v, writer)
          }
          writer.append("}")
          writer.flush()
        case i: TraversableOnce[_] =>
          writer.append("[")
          writer.flush()
          var first = true
          i.foreach { el =>
            if (first) first = false
            else {
              writer.append(",")
              writer.flush()
            }
            jsonWriteRecursive(el, writer)
          }
          writer.append("]")
          writer.flush()
        case p: Product => jsonWriteRecursive(p.productIterator, writer)
        case other => throw new IllegalArgumentException(s"Unsupported Conversion for type ${other.getClass}")
      }
    }
    jsonWriteRecursive(value, writer)
    writer.flush()
  }

  def html(status: StatusCode, value: Any, gzip: Boolean = true): HttpResponse = {
    val h = mutable.ListBuffer[HttpHeader]()
    h += Date(DateTime.now)
    h += `Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = html(value, gzip), headers = h.toList)
  }

  def html(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`text/html(UTF-8)`, gzip) { (writer) =>
      writer.append(value.toString)
      writer.close()
    }
  }

  def text(status: StatusCode, value: Any, gzip: Boolean = true): HttpResponse = {
    val h = mutable.ListBuffer[HttpHeader]()
    h += Date(DateTime.now)
    h += `Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = text(value, gzip), headers = h.toList)
  }

  def text(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`text/plain(UTF-8)`, gzip) { (writer) =>
      writer.append(value.toString)
      writer.close()
    }
  }

  private def encode(contentType: ContentType, gzip: Boolean )(f: (Writer) => Unit): MessageEntity = {

    val out = new ByteBufferOutputStream()
    val gout = if (gzip) new GZIPOutputStream(out) else out
    val writer = new OutputStreamWriter(gout)
    f(writer)
    val buffers = out.getBufferList

    buffers.size match {
      case 1 => HttpEntity(contentType, ByteString(buffers.get(0)))
      case _ => HttpEntity(contentType, Source(buffers.asScala.map(ByteString(_)).toList))
    }
  }
}
