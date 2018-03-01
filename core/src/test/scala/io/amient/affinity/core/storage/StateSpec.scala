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

package io.amient.affinity.core.storage

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.amient.affinity.Conf
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.util.EventTime
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class ExpirableValue(data: String, val eventTimeUnix: Long) extends AvroRecord with EventTime

class StateSpec extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  val specTimeout = 5 seconds

  val system = ActorSystem.create("test",
    ConfigFactory.parseMap(Map(
      Conf.Affi.Avro.Class.path -> classOf[MemorySchemaRegistry].getName,
      Conf.Affi.Node.Gateway.Http.Host.path -> "127.0.0.1",
      Conf.Affi.Node.Gateway.Http.Port.path -> "0"
    )).withFallback(ConfigFactory.defaultReference))

  override def afterAll {
    Await.ready(system.terminate(), 15 seconds)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  behavior of "State"

  it should "not allow writes and deletes in read-only state" in {
    val stateConf = State.StateConf(ConfigFactory.parseMap(Map(
      State.StateConf.MemStore.Class.path -> classOf[MemStoreSimpleMap].getName,
      State.StateConf.External.path -> "true"
    )))
    val state = State.create[Long, ExpirableValue]("read-only-store", 0, stateConf, 1, system)
    an[IllegalStateException] should be thrownBy (Await.result(state.insert(1L, ExpirableValue("one", 1)), specTimeout))
    an[IllegalStateException] should be thrownBy (Await.result(state.delete(1L), specTimeout))
  }

  it should "work without ttl" in {

    val stateConf = State.StateConf(ConfigFactory.parseMap(Map(
      State.StateConf.MemStore.Class.path -> classOf[MemStoreSimpleMap].getName
    )))
    val state = State.create[Long, ExpirableValue]("no-ttl-store", 0, stateConf, 1, system)

    val nowMs = System.currentTimeMillis()

    Await.result(Future.sequence(List(
      state.insert(1L, ExpirableValue("one", nowMs - 9000)),
      state.insert(2L, ExpirableValue("two", nowMs - 3000)),
      state.insert(3L, ExpirableValue("three", nowMs)))), specTimeout)
    state(1L) should be(Some(ExpirableValue("one", nowMs - 9000)))
    state(2L) should be(Some(ExpirableValue("two", nowMs - 3000)))
    state(3L) should be(Some(ExpirableValue("three", nowMs)))
    state.iterator.size should be(3L)
  }

  it should "clean expired entries when ttl set" in {
    val stateConf = State.StateConf(ConfigFactory.parseMap(Map(
      State.StateConf.MemStore.Class.path -> classOf[MemStoreSimpleMap].getName,
      State.StateConf.TtlSeconds.path -> 5
    )))

    val state = State.create[Long, ExpirableValue]("ttl-store", 0, stateConf, 1, system)

    val nowMs = System.currentTimeMillis()

    Await.result(Future.sequence(List(
      state.insert(1L, ExpirableValue("one", nowMs - 9000)),
      state.insert(2L, ExpirableValue("two", nowMs - 3000)),
      state.insert(3L, ExpirableValue("three", nowMs)))), specTimeout)
    state(1L) should be(None)
    state(2L) should be(Some(ExpirableValue("two", nowMs - 3000)))
    state(3L) should be(Some(ExpirableValue("three", nowMs)))
    state.iterator.size should be(2L)
    state.numKeys should be(2L)
  }


}

