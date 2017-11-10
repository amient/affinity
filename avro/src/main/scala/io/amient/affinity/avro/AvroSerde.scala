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

package io.amient.affinity.avro

import java.nio.ByteBuffer

import com.typesafe.config.Config
import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.avro.schema.AvroSchemaProvider
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

object AvroSerde {
  final val CONFIG_PROVIDER_CLASS = "affinity.avro.schema.provider.class"
  def create(config: Config): AvroSerde = {
    require(config.hasPath(CONFIG_PROVIDER_CLASS), "missing configuration " + CONFIG_PROVIDER_CLASS)
    val providerClass: Class[_ <: AvroSerde] = {
      val providerClassName = config.getString(CONFIG_PROVIDER_CLASS)
      Class.forName(providerClassName).asSubclass(classOf[AvroSerde])
    }

    val instance = try {
      providerClass.getConstructor(classOf[Config]).newInstance(config)
    } catch {
      case _: NoSuchMethodException => providerClass.newInstance()
    }
    instance.initialize()
    instance
  }
}

trait AvroSerde extends AbstractSerde[Any] with AvroSchemaProvider {


  override def close(): Unit = ()

  /**
    * Deserialize bytes to a concrete instance
    * @param bytes
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  override def fromBytes(bytes: Array[Byte]): Any = AvroRecord.read(bytes, this)

  val INT_SCHEMA = Schema.create(Schema.Type.INT)
  val BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN)
  val LONG_SCHEMA = Schema.create(Schema.Type.LONG)
  val FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT)
  val DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE)
  val STRING_SCHEMA = Schema.create(Schema.Type.STRING)
  val BYTES_SCHEMA = Schema.create(Schema.Type.BYTES)
  val NULL_SCHEMA = Schema.create(Schema.Type.NULL)

  /**
    * @param obj instance to serialize
    * @return serialized byte array
    */
  override def toBytes(obj: Any): Array[Byte] = {
    if (obj == null) null
    else {
      val schemaFQN = obj match {
        case record: IndexedRecord => record.getSchema.getFullName
        case _: Boolean => BOOLEAN_SCHEMA.getFullName
        case _: Byte => INT_SCHEMA.getFullName
        case _: Int => INT_SCHEMA.getFullName
        case _: Long => LONG_SCHEMA.getFullName
        case _: Float => FLOAT_SCHEMA.getFullName
        case _: Double => DOUBLE_SCHEMA.getFullName
        case _: String => STRING_SCHEMA.getFullName
        case _: ByteBuffer => BYTES_SCHEMA.getFullName
        case _ => throw new IllegalArgumentException("Unsupported mapping from Any to Avro Type")
      }

      (for (
        schemaId <- schema(schemaFQN);
        (_, schema) <- schema(schemaId)
      ) yield {
        AvroRecord.write(obj, schema, schemaId)
      }).getOrElse(throw new IllegalArgumentException(s"Schema not registered for $schemaFQN"))

    }
  }


}
