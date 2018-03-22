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

import java.io.{ByteArrayOutputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
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
    jsonWrite(value, out)
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
    encode(ContentTypes.`application/json`, gzip) { (out) =>
      jsonWrite(value, out)
      out.close()
    }
  }

  private def jsonWrite(value: Any, out: OutputStream): Unit = {

    def jsonWriteRec(value: Any): Unit = {
      value match {
        case null => out.write("null".getBytes)
        case None => jsonWriteRec(null)
        case Some(x) => jsonWrite(x, out)
        case j: JsonNode => out.write(j.toString.getBytes(StandardCharsets.UTF_8))
        case i: Iterable[_] if (i.size > 0 && i.head.isInstanceOf[(_, _)] && i.head.asInstanceOf[(_, _)]._1.isInstanceOf[String]) =>
          out.write("{".getBytes)
          out.flush()
          var first = true
          i.foreach { case (k, v) =>
            if (first) first = false
            else {
              out.write(",".getBytes)
              out.flush()
            }
            jsonWriteRec(k)
            out.write(":".getBytes)
            out.flush()
            jsonWrite(v, out)
          }
          out.write("}".getBytes)
          out.flush()
        case i: TraversableOnce[_] =>
          out.write("[".getBytes)
          out.flush()
          var first = true
          i.foreach { el =>
            if (first) first = false
            else {
              out.write(",".getBytes)
              out.flush()
            }
            jsonWrite(el, out)
          }
          out.write("]".getBytes)
          out.flush()

        case other => AvroJsonConverter.toJson(out, other, false)
      }
    }

    value match {
      case record: IndexedRecord =>
        val schema = record.getSchema
        out.write(("{\"type\":\"" + schema.getFullName + "\",\"data\":").getBytes(StandardCharsets.UTF_8))
        out.flush
        AvroJsonConverter.toJson(out, schema, value, false)
        out.write("}".getBytes(StandardCharsets.UTF_8))
        out.flush
      case other => jsonWriteRec(other)
    }
  }

  def html(status: StatusCode, value: Any, gzip: Boolean = true): HttpResponse = {
    val h = mutable.ListBuffer[HttpHeader]()
    h += Date(DateTime.now)
    h += `Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = html(value, gzip), headers = h.toList)
  }

  def html(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`text/html(UTF-8)`, gzip) { (out) =>
      out.write(value.toString.getBytes(StandardCharsets.UTF_8))
      out.close()
    }
  }

  def text(status: StatusCode, value: Any, gzip: Boolean = true): HttpResponse = {
    val h = mutable.ListBuffer[HttpHeader]()
    h += Date(DateTime.now)
    h += `Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    HttpResponse(status, entity = text(value, gzip), headers = h.toList)
  }

  def text(value: Any, gzip: Boolean): MessageEntity = {
    encode(ContentTypes.`text/plain(UTF-8)`, gzip) { (out) =>
      out.write(value.toString.getBytes(StandardCharsets.UTF_8))
      out.close()
    }
  }

  private def encode(contentType: ContentType, gzip: Boolean)(f: (OutputStream) => Unit): MessageEntity = {

    val out = new ByteBufferOutputStream()
    val gout = if (gzip) new GZIPOutputStream(out) else out
    f(gout)
    val buffers = out.getBufferList

    buffers.size match {
      case 1 => HttpEntity(contentType, ByteString(buffers.get(0)))
      case _ => HttpEntity(contentType, Source(buffers.asScala.map(ByteString(_)).toList))
    }
  }
}
