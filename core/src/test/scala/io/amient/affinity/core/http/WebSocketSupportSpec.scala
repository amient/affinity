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

import java.io.IOException
import java.net.URI
import java.util.concurrent.atomic.AtomicReference

import akka.actor.Props
import akka.http.scaladsl.model.HttpMethods.GET
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.IntegrationTestBase
import io.amient.affinity.core.actor.Controller.{CreateContainer, CreateGateway, GracefulShutdown}
import io.amient.affinity.core.actor.{Controller, Gateway, Partition, WebSocketSupport}
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.serde.avro._
import io.amient.affinity.ws.AvroWebSocketClient
import io.amient.affinity.ws.AvroWebSocketClient.AvroMessageHandler
import org.apache.avro.generic.GenericData
import org.scalatest.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class WebSocketSupportSpec extends IntegrationTestBase with Matchers {

  import system.dispatcher

  private val controller = system.actorOf(Props(new Controller), name = "controller")
  implicit val timeout = Timeout(10 seconds)

  controller ? CreateContainer("region", Props(new Partition() {
    val data = state[Int, Base]("test")

    override def handle: Receive = {
      case base: Base => data.update(base.id.id, base)
    }
  }))

  val httpPort = Await.result(controller ? CreateGateway(Props(new Gateway() with WebSocketSupport {
    override def handle: Receive = {
      case http@HTTP(GET, PATH("test"), _, response) =>
        avroWebSocket(http, service("region"),  "test", 1) {
          case c: Composite => System.err.println("WS Server custom avro message " + c)
        }
    }
  })) map {
    case port: Int => port
  }, timeout.duration)

  awaitClusterReady()

  override def afterAll: Unit = {
    controller ! GracefulShutdown()
    Await.ready(system.whenTerminated, 10 seconds)
    super.afterAll
  }

  "WebSocket channel" must {

    "throw exception for unknown type schema request" in {
      (try {
        val ws = new AvroWebSocketClient(URI.create(s"ws://127.0.0.1:$httpPort/test"), new AvroMessageHandler() {
          override def onMessage(message: scala.Any): Unit = ()
        })
        try {
          ws.getSchema("io.amient.affinity.core.serde.avro.WrongClass")
          false
        } finally {
          ws.close()
        }
      } catch {
        case e: IOException => true
      }) should be(true)
    }

    "retrieve valid schema for known type and send a receive objects according to that schama" in {
      val lastMessage = new AtomicReference[Any](null)
      val ws = new AvroWebSocketClient(URI.create(s"ws://127.0.0.1:$httpPort/test"), new AvroMessageHandler() {
        override def onMessage(message: scala.Any): Unit = {
          lastMessage.synchronized {
            lastMessage.set(message)
            lastMessage.notify()
          }
        }
      })
      try {
        val schema = ws.getSchema("io.amient.affinity.core.serde.avro.Base")
        schema should equal(AvroRecord.inferSchema(classOf[Base]))
        ws.send(Base(ID(1), Side.LEFT, Seq(ID(2))))
        lastMessage.synchronized {
          lastMessage.wait(1500)
          (lastMessage.get != null) should be(true)
        }
        val record = lastMessage.get.asInstanceOf[GenericData.Record]
        record.get("id").asInstanceOf[GenericData.Record].get("id") should be(1)
        record.get("side").toString should be("LEFT")
        record.get("seq").asInstanceOf[GenericData.Array[GenericData.Record]].size should be(1)
        record.get("seq").asInstanceOf[GenericData.Array[GenericData.Record]].get(0).get("id") should be(2)
      } finally {
        ws.close()
      }
    }
  }

}
