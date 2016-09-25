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

package io.amient.affinity.systemtests.core

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes.{OK, SeeOther}
import akka.http.scaladsl.model.{ContentTypes, HttpResponse, Uri, headers}
import akka.util.Timeout
import io.amient.affinity.core.ack._
import io.amient.affinity.core.actor.Gateway
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.http.ResponseBuilder
import io.amient.affinity.testutil.SystemTestBaseWithKafka
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

class MasterTransitionSystemTest1 extends FlatSpec with SystemTestBaseWithKafka with Matchers {

  val gateway = new TestGatewayNode(new Gateway {

    import context.dispatcher

    override def handle: Receive = {
      case HTTP(GET, PATH(key), _, response) =>
        implicit val timeout = Timeout(500 milliseconds)
        fulfillAndHandleErrors(response, ack(cluster, key), ContentTypes.`application/json`) {
          case value => ResponseBuilder.json(OK, value, gzip = false)
        }

      case HTTP(POST, PATH(key, value), _, response) =>
        implicit val timeout = Timeout(1500 milliseconds)
        fulfillAndHandleErrors(response, ack(cluster, (key, value)), ContentTypes.`application/json`) {
          case result => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/$key"))))
        }
    }
  })

  val region1 = new TestRegionNode(new MyTestPartition("test", "R1") {
    data.put("A", Some("initialValueA"))
  })
  val region2 = new TestRegionNode(new MyTestPartition("test", "R2") {
    data.put("B", Some("initialValueB"))
  })
  awaitRegions()

  override def afterAll(): Unit = {
    try {
      gateway.shutdown()
      region2.shutdown()
      region1.shutdown()
    } finally {
      super.afterAll()
    }
  }

  "Master Transition" should "not cause requests being dropped when ack(cluster, _) is used" in {

    gateway.http_sync(GET, "/A").entity should be(jsonStringEntity("initialValueA"))
    gateway.http_sync(GET, "/B").entity should be(jsonStringEntity("initialValueB"))
    gateway.http_sync(POST, "/A/updatedValueA").status.intValue should be(303)
    gateway.http_sync(GET, "/A").entity should be(jsonStringEntity("updatedValueA"))

    val errorCount = new AtomicLong(0L)
    val stopSignal = new AtomicBoolean(false)

    import scala.concurrent.ExecutionContext.Implicits.global

    val client = new Thread {

      override def run: Unit = {
        val random = new Random()
        val requests = scala.collection.mutable.ListBuffer[Future[String]]()
        while (!stopSignal.get) {
          Thread.sleep(1)
          if (isInterrupted) throw new InterruptedException
          val path = if (random.nextBoolean()) "/A" else "/B"
          requests += gateway.http(GET, path) map {
            case response => response.status.value
          } recover {
            case e: Throwable => e.getMessage
          }
        }
        try {
          val statuses = Await.result(Future.sequence(requests), 5 seconds).groupBy(x => x).map {
            case (status, list) => (status, list.length)
          }
          errorCount.set(requests.size - statuses("200 OK"))
        } catch {
          case e: Throwable => errorCount.set(requests.size)
        }

      }
    }
    client.start
    Thread.sleep(100)
    region1.shutdown()
    stopSignal.set(true)
    client.join()
    errorCount.get should be(0L)
  }


}
