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

import io.amient.affinity.avro.record.AvroRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.reflect.runtime.universe._

/**
  * This Serde can be used for internal topics of processors. These schemas won't be registered
  * in the schema registry.
  *
  * @tparam T
  */
class InternalKafkaAvroSerde[T: TypeTag] extends Serde[T] {
  val schema = AvroRecord.inferSchema[T]
    override def configure(configs: java.util.Map[String, _], isKey: Boolean) = ()
    override def close() = ()

    override def deserializer() = new Deserializer[T] {
      override def configure(configs: java.util.Map[String, _], isKey: Boolean) = ()
      override def close() = ()
      override def deserialize(topic: String, data: Array[Byte]) = AvroRecord.read(data, schema)
    }

    override def serializer() = new Serializer[T] {
      override def configure(configs: java.util.Map[String, _], isKey: Boolean) = ()
      override def close() = ()
      override def serialize(topic: String, data: T) = AvroRecord.write(data, schema)
    }

}
