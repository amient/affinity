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

package io.amient.affinity.example.data

import com.fasterxml.jackson.annotation.JsonIgnore
import io.amient.affinity.core.data.AvroRecord
import io.amient.util.TimeCryptoProofSHA256
import org.apache.avro.SchemaBuilder

object ConfigEntry {
  val schema = SchemaBuilder.record("ConfigEntry")
    .namespace("io.amient.affinity.example.data").fields()
    .name("val1").`type`().stringType().noDefault()
    .name("salt").`type`().stringType().noDefault()
    .endRecord()
}

final case class ConfigEntry(val1: String, @JsonIgnore salt: String) extends AvroRecord(ConfigEntry.schema) {
  @JsonIgnore val crypto = new TimeCryptoProofSHA256(salt)
  override def hashCode(): Int = val1.hashCode()

  override def equals(obj: scala.Any): Boolean = obj.isInstanceOf[ConfigEntry] &&
    obj.asInstanceOf[ConfigEntry].val1 == val1 && obj.asInstanceOf[ConfigEntry].salt == salt
}