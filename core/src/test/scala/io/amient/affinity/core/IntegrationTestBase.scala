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

import java.util.UUID
import java.util.concurrent.TimeoutException

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.GroupStatus
import io.amient.affinity.core.http.HttpExchange
import io.amient.affinity.{AffinityActorSystem, Conf}
import org.scalatest.{BeforeAndAfterAll, Matchers, Suite, WordSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.postfixOps

trait IntegrationTestBase extends WordSpecLike with BeforeAndAfterAll with Matchers {

  self: Suite =>

  val system: ActorSystem = AffinityActorSystem.create("IntegrationTestSystem",
    ConfigFactory.load("integrationtests")
      .withValue(Conf.Affi.SystemName.path, ConfigValueFactory.fromAnyRef(UUID.randomUUID().toString)))

  private val groupsReady = scala.collection.mutable.Set[String]()
  system.eventStream.subscribe(system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case GroupStatus(g, false) => {
        groupsReady.synchronized {
          groupsReady.add(g)
          groupsReady.notifyAll()
        }
      }
    }
  })), classOf[GroupStatus])

  override def afterAll: Unit = {
    Await.ready(system.terminate(), 15 seconds)
  }

  def awaitGroupReady(group: String): Unit = {
    val t = System.currentTimeMillis()
    groupsReady.synchronized {
      while (!groupsReady.contains(group)) {
        if (System.currentTimeMillis() - t > 30000) throw new TimeoutException("Service didn't start within 30 seconds")
        groupsReady.wait(100)
      }
    }
  }

  implicit val materializer = ActorMaterializer.create(system)

  def http_get(uri: Uri): String = {
    val promise = Promise[HttpResponse]()
    system.actorSelection("/user/controller/gateway") ! HttpExchange(HttpRequest(GET, uri), promise)
    val response = Await.result(promise.future flatMap (_.toStrict(2 second)), 2 seconds)
    Await.result(response.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
  }

}
