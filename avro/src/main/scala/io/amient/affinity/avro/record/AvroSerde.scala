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

package io.amient.affinity.avro.record

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.util

import com.typesafe.config.Config
import io.amient.affinity.avro.{AvroSchemaRegistry, MemorySchemaRegistry}
import io.amient.affinity.avro.record.AvroSerde.MAGIC
import io.amient.affinity.core.config.{Cfg, CfgCls, CfgStruct}
import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.util.ByteUtils
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, IndexedRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.util.ByteBufferInputStream

import scala.collection.JavaConversions._

object AvroSerde {

  private val MAGIC: Byte = 0

  object AbsConf extends AbsConf {
    override def apply(config: Config): AbsConf = new AbsConf().apply(config)
  }

  class AbsConf extends CfgStruct[AbsConf](Cfg.Options.IGNORE_UNKNOWN) {
    val Avro = struct("affinity.avro", new AvroConf)
  }

  object AvroConf extends AvroConf {
    override def apply(config: Config): AvroConf = new AvroConf().apply(config)
  }

  class AvroConf extends CfgStruct[AvroConf] {
    val Class: CfgCls[AvroSerde] = cls("schema.registry.class", classOf[AvroSerde], classOf[MemorySchemaRegistry])

    override protected def specializations(): util.Set[String] = {
      Set("schema.registry")
    }
  }

  def create(config: Config): AvroSerde = create(AbsConf(config).Avro)

  def create(conf: AvroConf): AvroSerde = {
    val registryClass: Class[_ <: AvroSerde] = conf.Class()
    try {
      registryClass.getConstructor(classOf[Config]).newInstance(conf.config())
    } catch {
      case _: NoSuchMethodException => registryClass.newInstance()
    }
  }

  def prefixLength(recordClass: Class[_ <: AvroRecord]): Int = {
    val schema = AvroRecord.inferSchema(recordClass)
    val fixedLen = schema.getFields.map(_.schema).takeWhile(_.getType == Schema.Type.FIXED).map(_.getFixedSize).sum
    require(fixedLen > 0, recordClass.getName + " doesn't have any leading fixed fields")
    5 + fixedLen
  }


}

trait AvroSerde extends AbstractSerde[Any] with AvroSchemaRegistry {

  override def close(): Unit = ()

  /**
    * Deserialize bytes to a concrete instance
    *
    * @param bytes
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  override def fromBytes(bytes: Array[Byte]): Any = read(bytes)

  /**
    * @param obj instance to serialize
    * @return serialized byte array
    */
  override def toBytes(obj: Any): Array[Byte] = {
    if (obj == null) null
    else {
      val (schemaId, schema) = from(obj)
      write(obj, schema, schemaId)
    }
  }

  def write(x: IndexedRecord, schemaId: Int): Array[Byte] = {
    write(x, x.getSchema, schemaId)
  }

  def write(value: Any, schema: Schema, schemaId: Int): Array[Byte] = {
    require(schemaId >= 0)
    value match {
      case null => null
      case record: AvroRecord if record._serializedInstanceBytes != null => record._serializedInstanceBytes
      case any: Any =>
        val valueOut = new ByteArrayOutputStream()
        try {
          valueOut.write(MAGIC)
          ByteUtils.writeIntValue(schemaId, valueOut)
          AvroRecord.write(value, schema, valueOut)
          valueOut.toByteArray
        } finally {
          valueOut.close
        }
    }
  }

  /**
    *
    * @param buf ByteBuffer version of the registered avro reader
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  def read(buf: ByteBuffer): Any = {
    if (buf == null) null else read(new ByteBufferInputStream(List(buf)))
  }

  /**
    *
    * @param bytes
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  def read(bytes: Array[Byte]): Any = {
    if (bytes == null) null else read(new ByteArrayInputStream(bytes)) match {
      case a: AvroRecord => a._serializedInstanceBytes = bytes; a
      case other => other
    }
  }


  /**
    *
    * @param bytesIn InputStream implementation for the registered avro reader
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  def read(bytesIn: InputStream): Any = {
    require(bytesIn.read() == AvroSerde.MAGIC)
    val schemaId = ByteUtils.readIntValue(bytesIn)
    require(schemaId >= 0)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytesIn, null)
    val writerSchema = getSchema(schemaId)
    val (_, readerSchema) = getRuntimeSchema(writerSchema)
    //http://avro.apache.org/docs/1.7.2/api/java/org/apache/avro/io/parsing/doc-files/parsing.html
    val reader = new GenericDatumReader[Any](writerSchema, readerSchema)
    val record = reader.read(null, decoder)
    AvroRecord.read(record, readerSchema)
  }

  /**
    *
    * @param recordClass
    * @param keys
    * @return
    */
  def prefix(recordClass: Class[_ <: AvroRecord], keys: String*): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val schema = AvroRecord.inferSchema(recordClass)
    val schemaId: Int = register(recordClass)
    output.write(0) //magic byte
    ByteUtils.writeIntValue(schemaId, output) //schema id
    //all used prefix keys
    keys.zip(schema.getFields.take(keys.length).map(_.schema.getFixedSize)).foreach {
      case (value: String, fixedLen: Int) =>
        output.write(AvroRecord.stringToFixed(value, fixedLen))
    }
    output.toByteArray
  }

}
