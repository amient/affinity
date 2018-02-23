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

package io.amient.affinity.kafka

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class KafkaAvroSerde extends SpecificKafkaAvroSerde[Any]

class SpecificKafkaAvroSerde[T] extends Serde[T] {

  val innerDeserializer = new Deserializer[T] {
    val inner = new KafkaAvroDeserializer

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = inner.configure(configs, isKey)

    override def deserialize(topic: String, data: Array[Byte]): T = inner.deserialize(topic, data).asInstanceOf[T]

    override def close(): Unit = inner.close()
  }

  val innerSerializer = new Serializer[T] {
    val inner = new KafkaAvroSerializer

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = inner.configure(configs, isKey)

    override def serialize(topic: String, data: T): Array[Byte] = inner.serialize(topic, data)

    override def close(): Unit = inner.close()
  }

  override def deserializer(): Deserializer[T] = innerDeserializer

  override def serializer(): Serializer[T] = innerSerializer

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
    innerDeserializer.configure(configs, isKey)
    innerSerializer.configure(configs, isKey)
  }

  override def close(): Unit = {
    innerDeserializer.close()
    innerSerializer.close()
  }

}
