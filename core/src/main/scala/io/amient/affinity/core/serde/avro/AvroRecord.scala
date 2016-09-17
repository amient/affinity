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

package io.amient.affinity.core.serde.avro

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, IndexedRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.Utf8
import org.apache.avro.{AvroRuntimeException, Schema}

import scala.collection.JavaConverters._

object AvroRecord {

  def write(x: IndexedRecord, schemaId: Int): Array[Byte] = {
    write(x, x.getSchema, schemaId)
  }

  def write(x: Any, schema: Schema, schemaId: Int = -1): Array[Byte] = {
    val valueOut = new ByteArrayOutputStream()
    try {
      val encoder = EncoderFactory.get().binaryEncoder(valueOut, null)
      val writer = new GenericDatumWriter[Any](schema)
      if (schemaId >= 0) encoder.writeInt(schemaId)
      writer.write(x, encoder)
      encoder.flush()
      valueOut.toByteArray
    } finally {
      valueOut.close
    }
  }


  def read[T](bytes: Array[Byte], cls: Class[T], schema: Schema): T = {
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
    read(bytes, cls, schema, decoder)
  }

  def read[T](bytes: Array[Byte], cls: Class[T], schema: Schema, decoder: BinaryDecoder): T = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val record: GenericRecord = reader.read(null, decoder)
    readRecord(record, cls).asInstanceOf[T]
  }

  def read[T](bytes: Array[Byte], cls: Class[T], schemaRegistry: (Int) => (Class[_], Schema)): T = {
    if (bytes == null) null.asInstanceOf[T] else {
      val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
      val schemaId = decoder.readInt()
      require(schemaId >= 0)
      val (cls2, schema) = schemaRegistry(schemaId)
      read(bytes, cls, schema, decoder)
    }
  }

  def read(bytes: Array[Byte], schemaRegistry: (Int) => (Class[_], Schema)): AnyRef = {
    if (bytes == null) null.asInstanceOf[AnyRef] else {
      val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
      val schemaId = decoder.readInt()
      require(schemaId >= 0)
      val (cls, schema) = schemaRegistry(schemaId)
      read(bytes, cls, schema, decoder).asInstanceOf[AnyRef]
    }
  }



  private val cache = scala.collection.mutable.Map[String, Class[_]]()
  private def readDatum(datum: Any, cls: Class[_], schema: Schema): AnyRef = {

    def classCache(fullClassName: String): Class[_] = {
      cache.get(fullClassName) match {
        case Some(cls) => cls
        case None =>
          val cls = Class.forName(fullClassName)
          cache.put(fullClassName, cls)
          cls
      }
    }
    schema.getType match {
      case BOOLEAN => new java.lang.Boolean(datum.asInstanceOf[Boolean])
      case INT => new java.lang.Integer(datum.asInstanceOf[Int])
      case NULL => null
      case FLOAT => new java.lang.Float(datum.asInstanceOf[Float])
      case DOUBLE => new java.lang.Double(datum.asInstanceOf[Double])
      case LONG => new java.lang.Long(datum.asInstanceOf[Long])
      case FIXED | BYTES => datum.asInstanceOf[Array[Byte]]
      case STRING => String.valueOf(datum.asInstanceOf[Utf8])
      case RECORD => readRecord(datum.asInstanceOf[GenericRecord], classCache(schema.getFullName))
      case ENUM => classCache(schema.getFullName).getMethod("withName", classOf[String])
          .invoke(null, datum.asInstanceOf[EnumSymbol].toString)
      case MAP => datum.asInstanceOf[java.util.Map[Utf8, _]].asScala.toMap
          .map{ case (k,v)  => (
            k.toString,
            readDatum(v, classCache(schema.getValueType.getFullName), schema.getValueType))}
      case ARRAY => val iterable = datum.asInstanceOf[java.util.Collection[_]].asScala
          .map(item => readDatum(item, classCache(schema.getElementType.getFullName), schema.getElementType))
        if (cls.isAssignableFrom(classOf[Set[_]])) {
          iterable.toSet
        } else if (cls.isAssignableFrom(classOf[List[_]])) {
          iterable.toList
        } else if (cls.isAssignableFrom(classOf[Vector[_]])) {
          iterable.toVector
        } else if (cls.isAssignableFrom(classOf[IndexedSeq[_]])) {
          iterable.toIndexedSeq
        } else if (cls.isAssignableFrom(classOf[Seq[_]])) {
          iterable.toSeq
        } else {
          iterable
        }
      case UNION=> throw new NotImplementedError("Avro Unions are not supported")
    }
  }

  private def readRecord[T](record: GenericRecord, cls: Class[T]): AnyRef = {
    val c = cls.getConstructors()(0)
    val params = c.getParameterTypes
    val arguments: Seq[Object] = record.getSchema.getFields.asScala.map { field =>
      val datum: AnyRef = record.get(field.pos)
      val param: Class[_] = params(field.pos)
      readDatum(datum, param, field.schema)
    }
    c.newInstance(arguments: _*).asInstanceOf[AnyRef]
  }


}

abstract class AvroRecord(@JsonIgnore schema: Schema) extends SpecificRecord with java.io.Serializable {

  private val schemaFields = schema.getFields
  private val params = getClass.getConstructors()(0).getParameters
  require(params.length == schemaFields.size,
    s"number of constructor arguments (${params.length}) is not equal to schema field count (${schemaFields.size})")

  @transient private val declaredFields = getClass.getDeclaredFields
  @transient private val fields = params.zipWithIndex.map { case (param, pos) => {
    val field = declaredFields(pos)
    require(param.getType == field.getType,
      s"field `${field.getType}` at position $pos doesn't match expected `$param`")
    field.setAccessible(true)
    pos -> field
  }
  }.toMap

  override def getSchema: Schema = schema

  final override def get(i: Int): AnyRef = {
    val schemaField = schema.getFields.get(i)
    val field = fields(i)
    schemaField.schema().getType match {
      case ARRAY => field.get(this).asInstanceOf[Iterable[_]].asJava
      case ENUM => new EnumSymbol(schemaField.schema, field.get(this))
      case MAP => field.get(this).asInstanceOf[Map[String, _]].asJava
      case _ => field.get(this)
    }
  }

  final override def put(i: Int, v: scala.Any): Unit = {
    throw new AvroRuntimeException("Scala AvroRecorod is immutable")
  }
}
