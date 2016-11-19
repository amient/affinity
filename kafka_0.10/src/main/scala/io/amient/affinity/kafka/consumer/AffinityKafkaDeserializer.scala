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

package io.amient.affinity.kafka.consumer

import io.amient.affinity.core.serde.avro.AvroRecord
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericContainer
import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object AffinityKafkaAvroDeserializer {
  def create[T: TypeTag](props: Map[String, Any], isKey: Boolean): Deserializer[T] = {
    if (props.contains("schema.registry.url")) {
      new TypedKafkaAvroDeserializer(props, isKey)
    } else {
      //TODO #21 implement Kafka Deserializer for ZkAvroSchemaProvider
      throw new RuntimeException("schema.registry configuration is missing in the consumer properties")
    }
  }
}
//TODO #33 port this class to to java8
class TypedKafkaAvroDeserializer[T: TypeTag](props: Map[String, Any], isKey: Boolean) extends Deserializer[T] {

  val internal = new KafkaAvroDeserializer()

  internal.configure(props.asJava, isKey)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean) = {
    internal.configure(configs, isKey)
  }

  override def close(): Unit = internal.close

  override def deserialize(topic: String, bytes: Array[Byte]): T = {
    internal.deserialize(topic, bytes) match {
      case container: GenericContainer => AvroRecord.read[T](container)
      case other => other.asInstanceOf[T]
    }
  }

}