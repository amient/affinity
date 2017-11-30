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

package io.amient.affinity.core.transaction

import akka.actor.Props
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.actor.Controller.{CreateContainer, CreateGateway, GracefulShutdown}
import io.amient.affinity.core.actor.{Controller, GatewayHttp, Partition}
import io.amient.affinity.core.http.RequestMatchers.{HTTP, INT, PATH, QUERY}
import io.amient.affinity.core.{IntegrationTestBase, ack}
import org.scalatest.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class TransactionSpec extends IntegrationTestBase with Matchers {

  import system.dispatcher

  private val controller = system.actorOf(Props(new Controller), name = "controller")
  implicit val timeout = Timeout(10 seconds)

  controller ? CreateContainer("region", List(0,1,2,3), Props(new Partition() {
    val data = state[TestKey, TestValue]("test")

    override def handle: Receive = {
      case request@TestKey(id) => sender.reply(request) {
        data(request)
      }

      case request@AddItem(key, item) => sender.replyWith(request) {
        data.update(key) {
          case None => (Some(request), Some(TestValue(List(item))), TestValue(List()))
          case Some(prev) if (prev.items.contains(item)) => (None, Some(prev), prev)
          case Some(_) if (item == 106) => throw new IllegalArgumentException("Simulated transaction failure")
          case Some(prev) => (Some(request), Some(prev.withAddedItem(item)), prev)
        }
      }

      case request@RemoveItem(key, item) => sender.replyWith(request) {
        data.update(key) {
          case None => (None, Some(TestValue(List())), TestValue(List()))
          case Some(prev) if (!prev.items.contains(item)) => (None, Some(prev), prev)
          case Some(prev) if (item == 102) =>
            throw new IllegalArgumentException("Simulated transaction failure")
          case Some(prev) => (Some(request), Some(prev.withRemovedItem(item)), prev)
        }
      }
    }

  }))

  val httpPort = Await.result(controller ? CreateGateway(Props(new GatewayHttp {
    val regionService = service("region")
    override def handle: Receive = {
      case http@HTTP(GET, PATH("get", INT(id)), _, response) =>
        delegateAndHandleErrors(response, regionService ack TestKey(id)) {
          case Some(value) => HttpResponse(OK, entity = value.toString)
          case None => HttpResponse(NotFound)
        }
      case http@HTTP(GET, PATH("add", INT(id)), QUERY(("items", items)), response) =>
        val t = Transaction(regionService) { transaction =>
          def recAddItem(itemsToAdd: List[Int]): Future[TestValue] = {
            transaction execute AddItem(TestKey(id), itemsToAdd.head) flatMap {
              case v: TestValue => if (itemsToAdd.tail.isEmpty) Future.successful(v) else recAddItem(itemsToAdd.tail)
            }
          }
          recAddItem(items.split(",").toList.map(_.toInt))
        }

        delegateAndHandleErrors(response, t) {
          case prev => HttpResponse(OK, entity = prev.toString)
        }

      case http@HTTP(GET, PATH("remove", INT(id)), QUERY(("items", items)), response) =>
        val t = Transaction(regionService) { transaction =>
          def recAddItem(itemsToRemove: List[Int]): Future[TestValue] = {
            transaction execute RemoveItem(TestKey(id), itemsToRemove.head) flatMap {
              case v: TestValue => if (itemsToRemove.tail.isEmpty) Future.successful(v) else recAddItem(itemsToRemove.tail)
            }
          }
          recAddItem(items.split(",").toList.map(_.toInt))
        }

        delegateAndHandleErrors(response, t) {
          case prev => HttpResponse(OK, entity = prev.toString)
        }
    }
  })) map {
    case port: Int => port
  }, timeout.duration)

  awaitServiceReady("region")

  override def afterAll: Unit = {
    controller ! GracefulShutdown()
    Await.ready(system.whenTerminated, 10 seconds)
    super.afterAll
  }


  "A Transaction Failure" must {
    "should revert all successful instructions with their inverse" in {
      http_get(s"/add/1?items=100") should be("TestValue(List())")
      http_get(s"/add/1?items=101,102") should be("TestValue(List(100, 101))")
      http_get(s"/get/1") should be("TestValue(List(100, 101, 102))")
      //item 106 will simulate an error while 103,104,105 where already successful they should be reverted
      http_get(s"/add/1?items=103,104,105,106,107")
      http_get(s"/get/1") should be("TestValue(List(100, 101, 102))")
      http_get(s"/remove/1?items=101,102")
      //item 102 will simulate an error while 101 was already successfully removed so it should be added back by revert
      http_get(s"/get/1") should be("TestValue(List(100, 102, 101))")
      http_get(s"/remove/1?items=100")
      http_get(s"/get/1") should be("TestValue(List(102, 101))")
    }
  }
}
