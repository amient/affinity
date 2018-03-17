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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpResponse, StatusCode, Uri, headers}
import akka.http.scaladsl.model.StatusCodes.{OK, SeeOther}
import akka.util.Timeout
import com.typesafe.config.ConfigValueFactory
import io.amient.affinity.Conf
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.GatewayHttp
import io.amient.affinity.core.cluster.FailoverTestPartition.{GetValue, PutValue}
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.EmbeddedKafka
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Random, Success, Try}


class Failover2Spec extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers {

  val specTimeout = 15 seconds

  override def numPartitions = 2

  def config = configure("systemtests", Some(zkConnect), Some(kafkaBootstrap))
    .withValue(Conf.Affi.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[MemorySchemaRegistry].getName))

  val node1 = new Node(config)
  node1.startGateway(new GatewayHttp {

    import context.dispatcher

    implicit val scheduler = context.system.scheduler

    val keyspace1 = keyspace("keyspace1")

    override def handle: Receive = {
      case HTTP(GET, PATH(key), _, response) => handleWith(response) {
        implicit val timeout = Timeout(specTimeout / 5)
        keyspace1 ack GetValue(key) map {
          case valueOption => Encoder.json(OK, valueOption, gzip = false)
        }
      }

      case HTTP(POST, PATH(key, value), _, response) => handleWith(response) {
        implicit val timeout = Timeout(specTimeout / 5)
        keyspace1 ack PutValue(key, value) map {
          case result => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/$key"))))
        }
      }
    }
  })

  val node2 = new Node(config)
  val node3 = new Node(config)

  override def beforeAll(): Unit = try {
    node2.startContainer("keyspace1", List(0, 1), new FailoverTestPartition("consistency-test"))
    node3.startContainer("keyspace1", List(0, 1), new FailoverTestPartition("consistency-test"))
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

  "Master Transition" should "not lead to inconsistent state" in {
    val requestCount = new AtomicLong(0L)
    val expected = new ConcurrentHashMap[String, String]()
    import scala.concurrent.ExecutionContext.Implicits.global

    val random = new Random()
    val requests = scala.collection.mutable.ListBuffer[Future[Try[StatusCode]]]()
    for (i <- (1 to 250)) {
      val key = random.nextInt.toString
      val value = random.nextInt.toString
      requests += node1.http(POST, s"/$key/$value") map {
        case response =>
          if (i == 100) {
            //after a few writes have succeeded kill one node
            node2.shutdown()
          }
          expected.put(key, value)
          Success(response.status)
      } recover {
        case e: Throwable => Failure(e)
      }
    }
    requestCount.set(requests.size)
    Await.result(Future.sequence(requests), specTimeout).foreach(_ should be(Success(SeeOther)))

    expected.asScala.foreach { case (key, value) =>
      val y = Await.result(node1.http(GET, s"/$key").map { response => response.entity }, specTimeout / 3)
      val x = jsonStringEntity(value)
      y should be(x)
    }
  }

}
