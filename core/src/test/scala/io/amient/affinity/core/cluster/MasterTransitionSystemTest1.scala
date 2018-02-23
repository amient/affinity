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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpResponse, Uri, headers}
import akka.util.Timeout
import com.typesafe.config.ConfigValueFactory
import io.amient.affinity.Conf
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.GatewayHttp
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.storage.{LogStorage, State}
import io.amient.affinity.core.util.MyTestPartition.{GetValue, PutValue}
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.EmbeddedKafka
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Random

class MasterTransitionSystemTest1 extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers {

  override def numPartitions = 2

  import scala.concurrent.ExecutionContext.Implicits.global

  def config = configure("systemtests", Some(zkConnect), Some(kafkaBootstrap))
    .withValue(Conf.Affi.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[MemorySchemaRegistry].getName))

  val gateway = new TestGatewayNode(config, new GatewayHttp {

    import context.dispatcher

    implicit val scheduler = context.system.scheduler

    val keyspace1 = keyspace("keyspace1")

    override def handle: Receive = {
      case HTTP(GET, PATH("status"), _, response) =>
        handleAsJson(response, describeKeyspaces)

      case HTTP(GET, PATH(key), _, response) =>
        implicit val timeout = Timeout(500 milliseconds)
        delegateAndHandleErrors(response, keyspace1 ack GetValue(key)) {
          _ match {
            case None => HttpResponse(NotFound)
            case Some(value) => Encoder.json(OK, value, gzip = false)
          }
        }

      case HTTP(POST, PATH(key, value), _, response) =>
        implicit val timeout = Timeout(1500 milliseconds)
        delegateAndHandleErrors(response, keyspace1 ack PutValue(key, value)) {
          case result => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/$key"))))
        }
    }
  })

  import gateway._

  val region1 = new Node(config)
  val region2 = new Node(config)
  gateway.awaitClusterReady {
    val stateConf = Conf(config).Affi.Keyspace("keyspace1").State("consistency-test")
    val p0 = State.create[String, String]("consistency-test", 0, stateConf, 2, system)
    val p1 = State.create[String, String]("consistency-test", 1, stateConf, 2, system)
    p0.boot()
    p1.boot()
    Await.result(Future.sequence(List(
      p0.replace("A", "initialValueA"),
      p0.replace("B", "initialValueB"),
      p1.replace("A", "initialValueA"),
      p1.replace("B", "initialValueB"))), 1 second)

    region1.startContainer("keyspace1", List(0, 1), new MyTestPartition("consistency-test"))
    region2.startContainer("keyspace1", List(0, 1), new MyTestPartition("consistency-test"))
  }

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

    http_get(uri("/A")).entity should be(jsonStringEntity("initialValueA"))
    http_get(uri("/B")).entity should be(jsonStringEntity("initialValueB"))
    http_post(uri("/A/updatedValueA")).status.intValue should be(303)
    http_get(uri("/A")).entity should be(jsonStringEntity("updatedValueA"))

    val errorCount = new AtomicLong(0L)
    val stopSignal = new AtomicBoolean(false)

    val client = new Thread {

      override def run: Unit = {
        val random = new Random()
        val requests = scala.collection.mutable.ListBuffer[Future[String]]()
        while (!stopSignal.get) {
          Thread.sleep(1)
          if (isInterrupted) throw new InterruptedException
          val uri = gateway.uri(if (random.nextBoolean()) "/A" else "/B")
          requests += http(GET, uri) map {
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
          case e: Throwable =>
            e.printStackTrace()
            errorCount.set(requests.size)
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
