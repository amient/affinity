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

package io.amient.affinity.kafka.producer

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object AffinityKafkaAvroSerializer {
  def create[T: TypeTag](props: Map[String, Any], isKey: Boolean): Serializer[T] = {
    if (props.contains("schema.registry.url")) {
      new TypedKafkaAvroSerializer(props, isKey)
    } else {
      //TODO #21 implement ZkAvroSchemaProvider Serializer
      throw new RuntimeException("schema.registry configuration is missing in the consumer properties")
    }
  }
}

class TypedKafkaAvroSerializer[T: TypeTag](props: Map[String, Any], isKey: Boolean) extends Serializer[T] {

  val newSubject = rootMirror.runtimeClass(typeOf[T]).getName

  val internal = new KafkaAvroSerializer() {
    override protected def serializeImpl(subject: String, data: Any): Array[Byte] = {
      //internal registry will lookup the schema by <topic>-value or <topic>-key so we need to intercept
      super.serializeImpl(newSubject, data)
    }
  }

  internal.configure(props.asJava, isKey)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean) = {
    internal.configure(configs, isKey)
  }

  override def close(): Unit = internal.close

  override def serialize(topic: String, data: T): Array[Byte] = {
    internal.serialize(topic, data)
  }

}
