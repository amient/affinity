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

import java.io.{StringWriter, Writer}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericRecordBuilder, IndexedRecord}
import org.apache.avro.util.Utf8
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.{JsonFactory, JsonGenerator, JsonNode}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.util.Try

object AvroJsonConverter {

  private val jfactory = new JsonFactory()

  def toJson(data: Any): String = {
    val out = new StringWriter()
    toJson(out, data)
    out.toString()
  }

  def toJson(writer: Writer, data: Any): Unit = toJson(writer, AvroRecord.inferSchema(data), data)

  def toJson(out: Writer, schema: Schema, data: Any): Unit = {
    val gen: JsonGenerator = jfactory.createJsonGenerator(out)

    //TODO use AvroExtractors with encoder instead of repeating the extraction logic
    def generate(datum: Any, schemas: List[Schema]): Unit = {
      require(schemas.size > 0, s"No schemas provided for datum: ${datum.getClass}")

      def typeIsAllowed(t: Schema.Type) = schemas.exists(_.getType == t)

      schemas.map(_.getType) match {
        case Schema.Type.UNION :: Nil => generate(datum, schemas(0).getTypes.toList)
        case _ => datum match {
          case null if typeIsAllowed(Schema.Type.NULL) => gen.writeNull()
          case null => throw new IllegalArgumentException("Illegal null value for schemas: " + schemas.map(_.getType).mkString(","))
          case _ if typeIsAllowed(Schema.Type.ENUM) => gen.writeString(datum.toString)
          case b: Boolean if typeIsAllowed(Schema.Type.BOOLEAN) => gen.writeBoolean(b)
          case b: Byte if typeIsAllowed(Schema.Type.INT) => gen.writeNumber(b)
          case i: Int if typeIsAllowed(Schema.Type.INT) => gen.writeNumber(i)
          case l: Long if typeIsAllowed(Schema.Type.LONG) => gen.writeNumber(l)
          case f: Float if typeIsAllowed(Schema.Type.FLOAT) => gen.writeNumber(f)
          case d: Double if typeIsAllowed(Schema.Type.DOUBLE) => gen.writeNumber(d)
          case s: String if typeIsAllowed(Schema.Type.STRING) => gen.writeString(s)
          case s: Utf8 if typeIsAllowed(Schema.Type.STRING) => gen.writeString(s.toString)
          case b: java.nio.ByteBuffer if typeIsAllowed(Schema.Type.BYTES) => gen.writeBinary(b.array()) //FIXME #123
          case b: Array[Byte] if typeIsAllowed(Schema.Type.FIXED) => gen.writeBinary(b)
          case r: IndexedRecord if typeIsAllowed(Schema.Type.RECORD) =>
            gen.writeStartObject()
            val s = schemas.find(_.getType == Schema.Type.RECORD).get
            s.getFields.zipWithIndex.foreach { case (f, i) =>
              gen.writeFieldName(f.name())
              generate(r.get(i), List(f.schema()))
            }
            gen.writeEndObject()
          case i: Seq[_] if typeIsAllowed(Schema.Type.ARRAY) =>
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)
            gen.writeStartArray()
            i.foreach(generate(_, s))
            gen.writeEndArray()
          case i: Iterable[_] if typeIsAllowed(Schema.Type.ARRAY) =>
            val s = schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)
            gen.writeStartArray()
            i.toSeq.foreach(generate(_, s))
            gen.writeEndArray()
          case w: java.util.AbstractCollection[_] if schemas.exists(_.getType == Schema.Type.ARRAY) =>
            gen.writeStartArray()
            w.toSeq.foreach(generate(_, schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)))
            gen.writeEndArray()
          case i: Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
            gen.writeStartObject()
            val s = schemas.filter(_.getType == Schema.Type.MAP).map(_.getValueType)
            i.toSeq.foreach { case (k, v) =>
              gen.writeFieldName(k.toString)
              generate(v, s)
            }
            gen.writeEndObject()
          case i: java.util.Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
            gen.writeStartObject()
            val s = schemas.filter(_.getType == Schema.Type.MAP).map(_.getValueType)
            i.toSeq.foreach { case (k, v) =>
              gen.writeFieldName(k.toString)
              generate(v, s)
            }
            gen.writeEndObject()
          case x => throw new IllegalArgumentException(s"Unsupported avro-json conversion for ${x.getClass} and the following schemas: { ${schemas.mkString(",")} }")
        }
      }
    }

    generate(data, List(schema))
    gen.flush()
  }

  private val mapper = new ObjectMapper()

  def toAvro(data: String, schema: Schema): Any = {

    //TODO    val parser = jfactory.createJsonParser(data)

    def to(data: JsonNode, schema: Schema): Any = {
      schema.getType match {
        case Schema.Type.NULL if data.isNull => null
        case Schema.Type.INT if data.isNumber => data.getIntValue
        case Schema.Type.LONG if data.isNumber => data.getLongValue
        case Schema.Type.FLOAT if data.isNumber => data.getDoubleValue.toFloat
        case Schema.Type.DOUBLE if data.isNumber => data.getDoubleValue
        case Schema.Type.STRING if data.isTextual => new Utf8(data.getTextValue)
        case Schema.Type.UNION if schema.getTypes.size == 2 && schema.getTypes.get(0).getType == Schema.Type.NULL =>
          schema.getTypes.toIterator.map(s => Try(to(data, s))).find(_.isSuccess).map(_.get).get
        case Schema.Type.ARRAY if data.isArray => data.getElements.map(x => to(x, schema.getElementType)).toList.asJava
        case Schema.Type.MAP if data.isObject =>
          val builder = Map.newBuilder[String, Any]
          schema.getFields foreach { field =>
            builder += field.name -> to(data.get(field.name), field.schema)
          }
          builder.result.asJava
        case Schema.Type.ENUM if data.isTextual => new EnumSymbol(schema, data.getTextValue)
        //TODO Schema.Type.BYTES => Array[Byte] =>
        //TODO Schema.Type.FIXED if runtime == "int" => ByteUtils..
        //TODO Schema.Type.FIXED if runtime == "long" => ByteUtils..
        //TODO Schema.Type.FIXED => AvroRecord.fixedToString(...)
        case Schema.Type.RECORD if data.isObject =>
          val builder = new GenericRecordBuilder(schema)
          schema.getFields foreach { field =>
            val d = data.get(field.name)
            builder.set(field, to(d, field.schema()))
          }
          builder.build()
        case _ => throw new IllegalArgumentException(s"Can't convert ${data} using schema: $schema")
      }
    }

    AvroRecord.read(to(mapper.readTree(data), schema), schema)
  }
}
