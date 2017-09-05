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

package io.amient.affinity.kafka

import java.util

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.core.serde.avro.AvroRecord
import io.amient.affinity.core.serde.avro.schema.{CfAvroSchemaRegistry, ZkAvroSchemaRegistry}
import io.amient.affinity.core.serde.{AbstractSerde, Serde}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericContainer
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

object KafkaSerde {

  def of[T](serde: AbstractSerde[T]): Deserializer[T] with Serializer[T] = {
    new Deserializer[T] with Serializer[T] {

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

      override def close(): Unit = ()

      override def deserialize(topic: String, data: Array[Byte]): T = {
        serde.fromBytes(data)
      }

      override def serialize(topic: String, data: T): Array[Byte] = {
        serde.toBytes(data)
      }
    }
  }

  def of[T: ClassTag](system: ActorSystem): Deserializer[T] with Serializer[T] = {
    of(Serde.of[T](system.settings.config))
  }

  def of[T: TypeTag](config: Config): Deserializer[T] with Serializer[T] = {
    if (config.hasPath(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE)) {
      new Serializer[T] with Deserializer[T] {

        val newSubject = rootMirror.runtimeClass(typeOf[T]).getName

        val props = new java.util.HashMap[String, Object]()
        props.put("schema.registry.url", config.getString(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE))

        val cfDeserializer = new KafkaAvroDeserializer() {
          configure(props, false)
        }

        val cfSerializer = new KafkaAvroSerializer() {
          configure(props, false)
          override protected def serializeImpl(subject: String, data: Any): Array[Byte] = {
            //internal registry will lookup the schema by <topic>-value or <topic>-key so we need to intercept
            super.serializeImpl(newSubject, data)
          }
        }

        override def configure(configs: java.util.Map[String, _], isKey: Boolean) = ()

        override def close(): Unit = {
          cfSerializer.close
          cfDeserializer.close
        }

        override def serialize(topic: String, data: T): Array[Byte] = {
          cfSerializer.serialize(topic, data)
        }

        override def deserialize(topic: String, data: Array[Byte]): T = {
          cfDeserializer.deserialize(topic, data) match {
            case container: GenericContainer => AvroRecord.read[T](container)
            case other => other.asInstanceOf[T]
          }
        }
      }
    } else if (config.hasPath(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_CONNECT)) {
      new Serializer[T] with Deserializer[T] {

        private var internal: ZkAvroSchemaRegistry = null

        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
          val config = ConfigFactory
            .parseMap(configs.map { case (k,v) => (k.replace("schema.registry.", "affinity.avro.zookeeper-schema-registry."), v)})
            .withFallback(ConfigFactory.defaultReference())

          internal = new ZkAvroSchemaRegistry(config)
        }

        override def close(): Unit = {
          if (internal != null) {
            internal.close()
            internal = null
          }
        }

        override def serialize(topic: String, data: T): Array[Byte] = {
          internal.toBytes(data)
        }

        override def deserialize(topic: String, data: Array[Byte]): T = {
          internal.fromBytes(data) match {
            case container: GenericContainer => AvroRecord.read[T](container)
            case any => any.asInstanceOf[T]
          }
        }
      }
    } else {
      throw new RuntimeException("schema.registry.url or schema.registry.zookeeper.connect configuration is missing in the serializer properties")
    }
  }

}
