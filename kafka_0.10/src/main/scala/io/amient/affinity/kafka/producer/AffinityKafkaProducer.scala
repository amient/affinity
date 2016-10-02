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

import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

class AffinityKafkaProducer[K: TypeTag, V: TypeTag](props: Map[String, Any]) extends KafkaProducer[K,V](
  (props ++ Map("partitioner.class" -> classOf[KafkaObjectHashPartitioner].getName))
    .mapValues(_.toString.asInstanceOf[AnyRef]).asJava,
  AffinityKafkaAvroSerializer.create[K](props, true),
  AffinityKafkaAvroSerializer.create[V](props, false)) {
}

