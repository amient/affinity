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

package io.amient.affinity.core.cluster

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpResponse, Uri, headers}
import akka.util.Timeout
import com.typesafe.config.ConfigValueFactory
import io.amient.affinity.Conf
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.GatewayHttp
import io.amient.affinity.core.cluster.FailoverTestPartition.{GetValue, PutValue}
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.EmbeddedKafka
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Random

class Failover1Spec extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers {

  val specTimeout = 15 seconds

  override def numPartitions = 2

  def config = configure("systemtests", Some(zkConnect), Some(kafkaBootstrap))
    .withValue(Conf.Affi.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[MemorySchemaRegistry].getName))

  val node1 = new Node(config)
  val node2 = new Node(config)
  val node3 = new Node(config)

  node1.startGateway(new GatewayHttp {

    implicit val scheduler = context.system.scheduler

    val keyspace1 = keyspace("keyspace1")

    import context.dispatcher

    override def handle: Receive = {
      case HTTP(GET, PATH(key), _, response) => handleWith(response) {
        implicit val timeout = Timeout(1 second)
        keyspace1 ack GetValue(key) map {
          _ match {
            case None => HttpResponse(NotFound)
            case Some(value) => Encoder.json(OK, value, gzip = false)
          }
        }
      }

      case HTTP(POST, PATH(key, value), _, response) => handleWith(response) {
        implicit val timeout = Timeout(1 second)
        keyspace1 ack PutValue(key, value) map {
          case result => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/$key"))))
        }
      }
    }
  })

  import scala.concurrent.ExecutionContext.Implicits.global

  override def beforeAll(): Unit = try {
    val stateConf = Conf(config).Affi.Keyspace("keyspace1").State("consistency-test")
    val p0 = State.create[String, String]("consistency-test", 0, stateConf, 2, node1.system)
    val p1 = State.create[String, String]("consistency-test", 1, stateConf, 2, node1.system)
    p0.boot()
    p1.boot()
    Await.result(Future.sequence(List(
      p0.replace("A", "initialValueA"),
      p0.replace("B", "initialValueB"),
      p1.replace("A", "initialValueA"),
      p1.replace("B", "initialValueB"))), 1 second)

    node2.startContainer("keyspace1", List(0, 1), new FailoverTestPartition("consistency-test"))
    node3.startContainer("keyspace1", List(0, 1), new FailoverTestPartition("consistency-test"))

    node1.awaitClusterReady
  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
    node1.shutdown()
    node2.shutdown()
    node3.shutdown()
  } finally {
    super.afterAll()
  }

  "Master Transition" should "not cause requests being dropped when ack(cluster, _) is used" in {

    node1.http_get("/A").entity should be(jsonStringEntity("initialValueA"))
    node1.http_get("/B").entity should be(jsonStringEntity("initialValueB"))
    node1.http_post("/A/updatedValueA").status.intValue should be(303)
    node1.http_get("/A").entity should be(jsonStringEntity("updatedValueA"))

    val errorCount = new AtomicInteger(0)
    val stopSignal = new AtomicBoolean(false)

    val client = new Thread {

      override def run: Unit = {
        val random = new Random()
        val requests = scala.collection.mutable.ListBuffer[Future[String]]()
        while (!stopSignal.get) {
          Thread.sleep(1)
          if (isInterrupted) throw new InterruptedException
          val path = if (random.nextBoolean()) "/A" else "/B"
          requests += node1.http(GET, path) map {
            case response => response.status.value
          } recover {
            case e: Throwable => e.getMessage
          }
        }
        try {
          val statuses = Await.result(Future.sequence(requests), specTimeout).groupBy(x => x).map {
            case (status, list) => (status, list.length)
          }
          errorCount.set(requests.size - statuses("200 OK"))
        } catch {
          case e: Throwable =>
            e.printStackTrace()
            errorCount.set(requests.size)
        }

      }
    }
    client.start
    Thread.sleep(100)
    node2.shutdown()
    stopSignal.set(true)
    client.join()
    errorCount.get should be(0L)
  }


}
