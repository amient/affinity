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

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.schema.AvroSchemaProvider
import io.amient.affinity.core.serde.AbstractSerde
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

object AvroSerde {
  final val CONFIG_PROVIDER_CLASS = "affinity.avro.schema.provider.class"
  def create(config: Config): AvroSerde = {
    require(config.hasPath(CONFIG_PROVIDER_CLASS), "missing configuration " + CONFIG_PROVIDER_CLASS)
    val providerClass: Class[_ <: AvroSerde] = {
      val providerClassName = config.getString(CONFIG_PROVIDER_CLASS)
      Class.forName(providerClassName).asSubclass(classOf[AvroSerde])
    }

    val instance = try {
      providerClass.getConstructor(classOf[Config]).newInstance(config.withFallback(ConfigFactory.defaultReference()))
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


  def getSchema(obj: Any): Schema = {
    obj match {
      case container: GenericContainer => container.getSchema
      case _: Boolean => AvroRecord.BOOLEAN_SCHEMA
      case _: Byte => AvroRecord.INT_SCHEMA
      case _: Int => AvroRecord.INT_SCHEMA
      case _: Long => AvroRecord.LONG_SCHEMA
      case _: Float => AvroRecord.FLOAT_SCHEMA
      case _: Double => AvroRecord.DOUBLE_SCHEMA
      case _: String => AvroRecord.STRING_SCHEMA
      case _: Array[Byte] => AvroRecord.BYTES_SCHEMA
      case _ => throw new IllegalArgumentException("Unsupported mapping from Any to Avro Type")
    }
  }

  /**
    * @param obj instance to serialize
    * @return serialized byte array
    */
  override def toBytes(obj: Any): Array[Byte] = {
    if (obj == null) null
    else {
      val s = getSchema(obj)
      val schemaId = getSchemaId(s).getOrElse(throw new IllegalArgumentException(s"Schema not registered: $s"))
      AvroRecord.write(obj, s, schemaId)
    }
  }


}
