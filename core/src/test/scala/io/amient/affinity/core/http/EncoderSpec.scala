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

package io.amient.affinity.core.http

import io.amient.affinity.avro.record.AvroRecord
import org.scalatest.{FlatSpec, Matchers}

case class TestRecord(a: String, b: Int) extends AvroRecord

class EncoderSpec extends FlatSpec with Matchers {

  "Encoder" should "decorate AvroRecord with type and data fields" in {
    Encoder.json(TestRecord("hello", 123)) should be ("{\"type\":\"io.amient.affinity.core.http.TestRecord\",\"data\":{\"a\":\"hello\",\"b\":123}}")
  }

  "Encoder" should "automatically format Maps" in {
    Encoder.json(Map("a" -> "hello", "b" -> 123)) should be ("{\"a\":\"hello\",\"b\":123}")
  }

  "Encoder" should "automatically format primitives and options" in {
    Encoder.json(null) should be ("null")
    Encoder.json(100) should be ("100")
    Encoder.json(1000L) should be ("1000")
    Encoder.json("text") should be ("\"text\"")
    Encoder.json(None) should be ("null")
    Encoder.json(Some(100)) should be ("100")
  }

}
