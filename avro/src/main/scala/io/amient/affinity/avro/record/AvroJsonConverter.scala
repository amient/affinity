/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.avro.record

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer

import io.amient.affinity.avro.record.AvroRecord.extract
import io.amient.affinity.core.util.ByteUtils
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecordBuilder}
import org.apache.avro.util.Utf8
import org.apache.avro.{LogicalTypes, Schema}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

object AvroJsonConverter {

  val UUID_TYPE = LogicalTypes.uuid()

  def toJson(data: Any, pretty: Boolean = false): String = {
    val out = new ByteArrayOutputStream()
    toJson(out, data, pretty)
    out.toString()
  }

  def toJson(out: OutputStream, data: Any, pretty: Boolean): Unit = toJson(out, AvroRecord.inferSchema(data), data, pretty)

  def toJson(out: OutputStream, schema: Schema, data: Any, pretty: Boolean): Unit = {
    val encoder: JsonEncoder = new JsonEncoder(schema, out, pretty)
    val writer = new GenericDatumWriter[Any](schema)
    writer.write(extract(data, List(schema)), encoder)
    encoder.flush()
  }

  private val mapper = new ObjectMapper()

  def toAvro[T: TypeTag](json: String): T = {
    return toAvro(json, AvroRecord.inferSchema[T]).asInstanceOf[T]
  }

  def toAvro(json: String, schema: Schema): Any = {

    def to(json: JsonNode, schema: Schema): Any = try {
      schema.getType match {
        case Schema.Type.NULL if json == null || json.isNull => null
        case Schema.Type.BOOLEAN if json.isBoolean => json.asBoolean
        case Schema.Type.INT if json.isNumber => json.asInt
        case Schema.Type.LONG if json.isNumber => json.asLong
        case Schema.Type.FLOAT if json.isNumber => json.asDouble.toFloat
        case Schema.Type.DOUBLE if json.isNumber => json.asDouble
        case Schema.Type.STRING if json == null => new Utf8()
        case Schema.Type.STRING if json.isTextual => new Utf8(json.asText)
        case Schema.Type.UNION if schema.getTypes.size == 2 && (schema.getTypes.get(0).getType == Schema.Type.NULL || schema.getTypes.get(1).getType == Schema.Type.NULL) =>
          schema.getTypes.asScala.map { s => Try(to(json, s)) }.find(_.isSuccess).map(_.get).get
        case Schema.Type.UNION =>
          val utype = json.fieldNames.next
          schema.getTypes.asScala.filter(_.getFullName == utype).map(s => Try(to(json.get(utype), s))).find(_.isSuccess).map(_.get).get
        case Schema.Type.ARRAY if json == null => java.util.Collections.emptyList[AnyRef]()
        case Schema.Type.ARRAY if json.isArray =>
          val javaList = new java.util.LinkedList[Any]()
          json.elements.asScala.foreach(x => javaList.add(to(x, schema.getElementType)))
          javaList
        case Schema.Type.MAP if json == null => java.util.Collections.emptyMap[Utf8, AnyRef]()
        case Schema.Type.MAP if json.isObject =>
          val javaMap = new java.util.HashMap[Utf8, Any]()
          json.fields.asScala foreach { entry =>
            javaMap.put(new Utf8(entry.getKey), to(entry.getValue, schema.getValueType))
          }
          javaMap
        case Schema.Type.ENUM if json.isTextual => new EnumSymbol(schema, json.asText)
        case Schema.Type.BYTES => ByteBuffer.wrap(json.binaryValue)
        case Schema.Type.FIXED if schema.getLogicalType == UUID_TYPE =>
          new GenericData.Fixed(schema, ByteUtils.parseUUID(json.asText))
        case Schema.Type.FIXED => new GenericData.Fixed(schema, json.binaryValue)
        case Schema.Type.RECORD if json.isObject =>
          val builder = new GenericRecordBuilder(schema)
          schema.getFields.asScala foreach { field =>
            try {
              if (json.has(field.name)) {
                val d = json.get(field.name)
                builder.set(field, to(d, field.schema()))
              }
            } catch {
              case e: Throwable => throw new IllegalArgumentException(s"Can't convert json field `$field` with value ${json.get(field.name)} using schema: ${field.schema}", e)
            }
          }
          try {
            builder.build()
          } catch {
            case e: Throwable => throw new IllegalArgumentException(s"Could not build ${schema}", e)
          }
        case x => throw new IllegalArgumentException(s"Unsupported schema type `$x`")
      }
    } catch {
      case e: Throwable => throw new IllegalArgumentException(s"Can't convert ${json} using schema: $schema", e)
    }

    AvroRecord.read(to(mapper.readTree(json), schema), schema)
  }
}
