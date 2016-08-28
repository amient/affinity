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

import akka.serialization.JSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter

abstract class AvroSerde extends JSerializer {

  private val registry1 = scala.collection.mutable.HashMap[Class[_], Int]()
  private val registry2 = scala.collection.mutable.ArrayBuffer[Schema]()
  protected def register(cls: Class[_], schema: Schema): Unit = {
    this.synchronized {
      registry1 += (cls -> registry2.size)
      registry2 += schema
    }
  }

  override def identifier: Int = 21

  override protected def fromBinaryJava(bytes: Array[Byte], manifest: Class[_]): AnyRef = {
    fromBytes(bytes, manifest.asSubclass(classOf[AnyRef]))
  }

  override def toBinary(o: AnyRef): Array[Byte] = toBytes(o)

  override def includeManifest: Boolean = false

  def toBytes[T](obj: T): Array[Byte] = {
    val valueOut = new ByteArrayOutputStream()
    try {
      val encoder = EncoderFactory.get().binaryEncoder(valueOut, null)
      if (obj.isInstanceOf[GenericRecord]) {
        val datum = obj.asInstanceOf[GenericRecord]
        registry1.get(obj.getClass) match {
          case None =>
              encoder.writeInt(-1)
              val writer = new GenericDatumWriter[GenericRecord](datum.getSchema)
              writer.write(datum, encoder)
          case Some(schemaId) =>
              val schema = registry2(schemaId)
              encoder.writeInt(schemaId)
              val writer = new GenericDatumWriter[GenericRecord](schema)
              writer.write(datum, encoder)
        }
      } else {
        val writer = new SpecificDatumWriter[T](obj.getClass.asInstanceOf[Class[T]])
        writer.write(obj, encoder)
      }
      encoder.flush()
      valueOut.toByteArray
    } finally {
      valueOut.close
    }
  }

  def fromBytes[T <: AnyRef](bytes: Array[Byte], cls: Class[T]): T = {
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null)
    decoder.readInt() match {
      case schemaId if (schemaId >= 0) =>
        val schema = registry2(schemaId)
        val reader = new GenericDatumReader[GenericRecord](schema)
        val constructor = cls.getConstructor(classOf[GenericRecord])
        constructor.newInstance(reader.read(null, decoder))
      case -1 =>
        val constructor = cls.getConstructor(classOf[(Schema => GenericRecord)])
        constructor.newInstance((schema: Schema) => {
          val reader = new GenericDatumReader[GenericRecord](schema)
          reader.read(null, decoder)
        })
    }
  }


}
