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

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8

import scala.collection.JavaConverters._

trait AvroExtractors {


  def extract(datum: Any, schemas: List[Schema]): Any = {

    def typeIsAllowed(t: Schema.Type) = schemas.find(_.getType == t)

    object AvroBoolean extends AvroExtractors {
      def unapply(b: Boolean): Option[java.lang.Boolean] = {
        typeIsAllowed(Schema.Type.BOOLEAN) map (_ => new java.lang.Boolean(b))
      }
    }

    object AvroByte extends AvroExtractors {
      def unapply(b: Byte): Option[java.lang.Integer] = {
        typeIsAllowed(Schema.Type.INT) map (_ => new Integer(b))
      }
    }

    object AvroInt extends AvroExtractors {
      def unapply(i: Int): Option[java.lang.Integer] = {
        typeIsAllowed(Schema.Type.INT) map (_ => new java.lang.Integer(i))
      }
    }

    object AvroLong extends AvroExtractors {
      def unapply(l: Long): Option[java.lang.Long] = {
        typeIsAllowed(Schema.Type.LONG) map (_ => new java.lang.Long(l))
      }
    }

    object AvroFloat extends AvroExtractors {
      def unapply(f: Float): Option[java.lang.Float] = {
        typeIsAllowed(Schema.Type.FLOAT) map (_ => new java.lang.Float(f))
      }
    }

    object AvroDouble extends AvroExtractors {
      def unapply(d: Double): Option[java.lang.Double] = {
        typeIsAllowed(Schema.Type.DOUBLE) map (_ => new java.lang.Double(d))
      }
    }

    object AvroBytes extends AvroExtractors {
      def unapply(b: Array[Byte]): Option[java.nio.ByteBuffer] = {
        typeIsAllowed(Schema.Type.BYTES) map (_ => ByteBuffer.wrap(b))
      }
    }

    object AvroString extends AvroExtractors {
      def unapply(s: Any): Option[String] = {
        typeIsAllowed(Schema.Type.STRING) map { _ =>
          s match {
            case str: String => str
            case utf: Utf8 => utf.toString
          }
        }
      }
    }

    object AvroArray extends AvroExtractors {
      def unapply(i: Iterable[Any]): Option[(java.lang.Iterable[Any])] = {
        typeIsAllowed(Schema.Type.ARRAY) map {
          schema => i.map(el => extract(el, List(schema.getElementType))).asJava
        }
      }
    }

    object AvroUnion extends AvroExtractors {
      def unapply(u: Any): Option[Any] = {
        schemas.map(_.getType) match {
          case Schema.Type.UNION :: Nil => Some(extract(u, schemas(0).getTypes.asScala.toList))
          case _ => None
        }
      }
    }

    object AvroEnum extends AvroExtractors {
      def unapply(s: Any): Option[EnumSymbol] = {
        typeIsAllowed(Schema.Type.ENUM) map (schema => new EnumSymbol(schema, s))
      }
    }


    require(schemas.size > 0, s"No schemas provided for datum: ${datum.getClass}")
    datum match {
      case r: IndexedRecord if typeIsAllowed(Schema.Type.RECORD).isDefined => r
      case null if typeIsAllowed(Schema.Type.NULL).isDefined => null
      case null => throw new IllegalArgumentException("Illegal null value for schemas: " + schemas.map(_.getType).mkString(","))
      case AvroEnum(e) => e
      case AvroBoolean(b) => b
      case AvroByte(b) => b
      case AvroInt(i) => i
      case AvroLong(l) => l
      case AvroFloat(f) => f
      case AvroDouble(d) => d
      case AvroString(s) => s
      case AvroBytes(b) => b
      case AvroArray(i) => i
      case AvroUnion(u) => u
      //          case w: java.util.AbstractCollection[_] if schemas.exists(_.getType == Schema.Type.ARRAY) =>
      //            gen.writeStartArray()
      //            w.toSeq.foreach(generate(_, schemas.filter(_.getType == Schema.Type.ARRAY).map(_.getElementType)))
      //            gen.writeEndArray()
      //          case i: Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
      //            gen.writeStartObject()
      //            val s = schemas.filter(_.getType == Schema.Type.MAP).map(_.getValueType)
      //            i.toSeq.foreach { case (k, v) =>
      //              gen.writeFieldName(k.toString)
      //              generate(v, s)
      //            }
      //            gen.writeEndObject()
      //          case i: java.util.Map[_, _] if typeIsAllowed(Schema.Type.MAP) =>
      //            gen.writeStartObject()
      //            val s = schemas.filter(_.getType == Schema.Type.MAP).map(_.getValueType)
      //            i.toSeq.foreach { case (k, v) =>
      //              gen.writeFieldName(k.toString)
      //              generate(v, s)
      //            }
      //            gen.writeEndObject()
      case x => throw new IllegalArgumentException(s"Unsupported avro extraction for ${x.getClass} and the following schemas: { ${schemas.mkString(",")} }")
    }
  }
}
