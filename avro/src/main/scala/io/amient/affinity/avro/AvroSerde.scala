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

  /**
    * @param obj instance to serialize
    * @return serialized byte array
    */
  override def toBytes(obj: Any): Array[Byte] = {
    if (obj == null) null
    else {
      val (s, schemaId) = getOrRegisterSchema(obj)
      AvroRecord.write(obj, s, schemaId)
    }
  }

  def getOrRegisterSchema(data: Any, subjectOption: String = null): (Schema, Int) = {
    val schema = AvroRecord.inferSchema(data)
    val subject = Option(subjectOption).getOrElse(schema.getFullName)
    val schemaId = getCurrentSchema(schema.getFullName) match {
      case Some((schemaId: Int, regSchema: Schema)) if regSchema == schema => schemaId
      case _ =>
        register(subject, schema)
        register(schema.getFullName, schema)
        initialize()
        getSchemaId(schema) match {
          case None => throw new IllegalStateException(s"Failed to register schema for $subject")
          case Some(id) => id
        }
    }
    (schema, schemaId)
  }

}
