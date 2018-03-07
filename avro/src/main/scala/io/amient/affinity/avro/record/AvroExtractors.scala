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

import java.lang
import java.nio.ByteBuffer

import io.amient.affinity.core.util.ByteUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericData, GenericFixed, IndexedRecord}
import org.apache.avro.util.Utf8

import scala.collection.JavaConverters._

trait AvroExtractors {


  def extract(datum: Any, schemas: List[Schema]): AnyRef = {

    def typeIsAllowed(t: Schema.Type) = schemas.find(_.getType == t)

    object AvroBoolean  {
      def unapply(b: Boolean): Option[java.lang.Boolean] = {
        typeIsAllowed(Schema.Type.BOOLEAN) map (_ => new java.lang.Boolean(b))
      }
    }

    object AvroByte {
      def unapply(b: Byte): Option[java.lang.Integer] = {
        typeIsAllowed(Schema.Type.INT) map (_ => new Integer(b))
      }
    }

    object AvroInt {
      def unapply(i: Int): Option[java.lang.Integer] = {
        typeIsAllowed(Schema.Type.INT) map (_ => new java.lang.Integer(i))
      }
    }

    object AvroLong {
      def unapply(l: Long): Option[java.lang.Long] = {
        typeIsAllowed(Schema.Type.LONG) map (_ => new java.lang.Long(l))
      }
    }

    object AvroFloat {
      def unapply(f: Float): Option[java.lang.Float] = {
        typeIsAllowed(Schema.Type.FLOAT) map (_ => new java.lang.Float(f))
      }
    }

    object AvroDouble {
      def unapply(d: Double): Option[java.lang.Double] = {
        typeIsAllowed(Schema.Type.DOUBLE) map (_ => new java.lang.Double(d))
      }
    }

    object AvroBytes {
      def unapply(b: Array[Byte]): Option[java.nio.ByteBuffer] = {
        typeIsAllowed(Schema.Type.BYTES) map (_ => ByteBuffer.wrap(b))
      }
    }

    object AvroString {
      def unapply(s: Any): Option[String] = {
        typeIsAllowed(Schema.Type.STRING) map { _ =>
          s match {
            case str: String => str
            case utf: Utf8 => utf.toString
          }
        }
      }
    }

    object AvroArray {
      def unapply(i: Iterable[Any]): Option[(java.lang.Iterable[AnyRef])] = {
        typeIsAllowed(Schema.Type.ARRAY) map {
          schema => i.map(el => extract(el, List(schema.getElementType))).asJava
        }
      }
    }

    object AvroMap {
      def unapply(m: Map[_, _]): Option[(java.util.Map[String, AnyRef])] = {
        typeIsAllowed(Schema.Type.MAP) map { schema =>
          m.map {
            case (k: String, v) => (k, extract(v, List(schema.getValueType)))
            case (k, v) => (k.toString, extract(v, List(schema.getValueType)))
          }.asJava
        }
      }
    }

    object AvroUnion {
      def unapply(u: Any): Option[AnyRef] = {
        schemas.map(_.getType) match {
          case Schema.Type.UNION :: Nil => u match {
            case None => Some(null.asInstanceOf[AnyRef])
            case Some(w) => Some(extract(w, schemas(0).getTypes.asScala.toList))
          }
          case _ => None
        }
      }
    }

    object AvroEnum {
      def unapply(s: Any): Option[EnumSymbol] = {
        typeIsAllowed(Schema.Type.ENUM) map (schema => new EnumSymbol(schema, s))
      }
    }

    object AvroFixed {
      def unapply(f: Any): Option[GenericFixed] = {
        typeIsAllowed(Schema.Type.FIXED) flatMap {
          //TODO case schema if schema.getProp("runtime") == "uuid" || f.isInstanceOf[UUID] =>
          case schema if schema.getProp("runtime") == "int" || f.isInstanceOf[Int] =>
            Some(new GenericData.Fixed(schema, ByteUtils.intValue(f.asInstanceOf[Int])))
          case schema if schema.getProp("runtime") == "long" || f.isInstanceOf[Long] =>
            Some(new GenericData.Fixed(schema, ByteUtils.longValue(f.asInstanceOf[Long])))
          case schema if f.isInstanceOf[String] =>
            val result: Array[Byte] = AvroRecord.stringToFixed(f.asInstanceOf[String], schema.getFixedSize)
            Some(new GenericData.Fixed(schema, result))
          case _ => None
        }
      }
    }

    datum match {
      case r: IndexedRecord if typeIsAllowed(Schema.Type.RECORD).isDefined => r
      case null if typeIsAllowed(Schema.Type.NULL).isDefined => null
      case null => throw new IllegalArgumentException("Illegal null value for schemas: " + schemas.map(_.getType).mkString(","))
      case AvroEnum(e: EnumSymbol) => e
      case AvroBoolean(b: lang.Boolean) => b
      case AvroByte(b: Integer) => b
      case AvroInt(i: Integer) => i
      case AvroLong(l: lang.Long) => l
      case AvroFloat(f: lang.Float) => f
      case AvroDouble(d: lang.Double) => d
      case AvroString(s: String) => s
      case AvroBytes(b: java.nio.ByteBuffer) => b
      case AvroArray(i: lang.Iterable[AnyRef]) => i
      case AvroUnion(u) => u
      case AvroFixed(b: GenericData.Fixed) => b
      case AvroMap(m) => m
      case x => throw new IllegalArgumentException(s"Unsupported avro extraction for ${x.getClass} and the following schemas: { ${schemas.mkString(",")} }")
    }
  }
}
