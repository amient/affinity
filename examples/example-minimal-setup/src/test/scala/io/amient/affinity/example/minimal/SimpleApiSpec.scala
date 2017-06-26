/*
 * Copyright 2016-2017 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.example.minimal

import com.typesafe.config.ConfigFactory
import io.amient.affinity.testutil.SystemTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._


class SimpleApiSpec extends FlatSpec with SystemTestBase with Matchers {

  val config = ConfigFactory.load("minimal-example-test")

  behavior of "Simple Api Gateway"

  it should "work without the http layer" in {

    new TestGatewayNode(configure(config)) with MyApiNode {
      awaitClusterReady {
        startContainer("simple-keyspace", List(0, 1), new MySimplePartition())
      }
      Await.result(getData("key1"), 1 second) should be (None)
      Await.result(putData("key1", "value1"), 1 second) should be(None)
      Await.result(getData("key1"), 1 second) should be(Some("value1"))
      Await.result(putData("key1", "value2"), 1 second) should be(Some("value1"))
      Await.result(getData("key1"), 1 second) should be(Some("value2"))
      shutdown()
    }
  }

}
