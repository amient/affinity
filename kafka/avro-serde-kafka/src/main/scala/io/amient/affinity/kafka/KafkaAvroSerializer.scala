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

import java.util

import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.record.{AvroRecord, AvroSerde}
import org.apache.kafka.common.serialization.Serializer

class KafkaAvroSerializer extends Serializer[Any] {

  var isKey: Boolean = false
  var serde: AvroSerde = null

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val config = ConfigFactory.parseMap(configs).getConfig("schema").atKey("schema").atPath(AvroSerde.AbsConf.Avro.path)
    this.serde = AvroSerde.create(config)
    this.isKey = isKey
  }

  override def serialize(topic: String, data: Any): Array[Byte] = {
    require(serde != null, "AvroSerde not configured")
    val subject = s"$topic-${if (isKey) "key" else "value"}"
    val (schemaId, objSchema) = serde.from(data, subject)
    serde.write(data, objSchema, schemaId)
  }

  override def close(): Unit = if (serde != null) serde.close()
}
