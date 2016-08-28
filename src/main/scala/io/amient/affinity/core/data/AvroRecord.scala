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

package io.amient.affinity.core.data

import java.io.ByteArrayOutputStream
import java.lang.reflect.Field

import io.amient.affinity.example.data.{ComponentV1, VertexV1}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, IndexedRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.Utf8
import org.apache.avro.{AvroRuntimeException, Schema}
import Schema.Type._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object AvroRecord extends App {

  def write(x: IndexedRecord): Array[Byte] = {
    val valueOut = new ByteArrayOutputStream()
    println("--------------")
    try {
      val encoder = EncoderFactory.get().binaryEncoder(valueOut, null)
      val writer = new GenericDatumWriter[IndexedRecord](x.getSchema)
      writer.write(x, encoder)
      encoder.flush()
      valueOut.toByteArray
    } finally {
      valueOut.close
    }
  }

  def read[T](cls:Class[T], schema: Schema): T = {
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val record: GenericRecord = reader.read(null, decoder)
    val c = cls.getConstructors()(0)
    val params = c.getParameterTypes
    val arguments: Seq[Object] = schema.getFields.asScala.map { field =>
      //TODO use TypeTag for conversions between param type and avro type
      val param = params(field.pos)
      val datum: AnyRef = record.get(field.pos)
      field.schema().getType() match {
        case INT => new java.lang.Integer(datum.asInstanceOf[Int])
        case STRING => String.valueOf(datum.asInstanceOf[Utf8])
        //TODO other cases
      }
    }
    c.newInstance(arguments: _*).asInstanceOf[T]
  }

  val myx = ComponentV1(VertexV1(0, "A"), Set(VertexV1(1, "B")))
  val bytes = write(myx)
  println(bytes.length)
  val z = read(classOf[ComponentV1], myx.schema)
  println(z)

}

abstract class AvroRecord(val schema: Schema) extends SpecificRecord {

  private val schemaFields = schema.getFields
  private val params = getClass.getConstructors()(0).getParameters
  require(params.length == schemaFields.size, "number of constructor arguments must be equal to schema field count")

  private val declaredFields = getClass.getDeclaredFields
  private val fields = params.zipWithIndex.map{ case (param, pos) => {
      val field = declaredFields(pos)
      require(param.getType == field.getType,
        s"field `${field.getType}` at position $pos doesn't match expected `$param`")
      field.setAccessible(true)
      pos -> field
  }}.toMap

  override def getSchema:Schema = schema

  override def get(i: Int): AnyRef = {
    val schemaField = schema.getFields.get(i)
    val field = fields(i)
    println(schemaField)
    schemaField.schema().getType match {
      //TODO must use a typetag of field.getType and match the conversion to case ARRAY => field.getType getField(field, field.getType)
      case _ => fields(i).get(this)
    }

  }

  override def put(i: Int, v: scala.Any): Unit = throw new AvroRuntimeException("Scala AvroRecorod is immutable")
}
