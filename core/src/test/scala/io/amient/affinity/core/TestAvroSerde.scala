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

package io.amient.affinity.core

import io.amient.affinity.core.serde.avro._
import io.amient.affinity.core.serde.avro.schema.EmbeddedAvroSchemaProvider
import io.amient.affinity.core.transaction.{TestKey, TestValue}

class TestAvroSerde extends AvroSerde with EmbeddedAvroSchemaProvider {
  register(classOf[Composite], AvroRecord.inferSchema(classOf[_V1_Composite]))
  register(classOf[Composite])
  register(classOf[Base])
  register(classOf[Boolean])
  register(classOf[Int])
  register(classOf[Long])
  register(classOf[Float])
  register(classOf[Double])
  register(classOf[String])
  register(classOf[Null])
  register(classOf[TestKey])
  register(classOf[TestValue])

  override def close(): Unit = ()
}