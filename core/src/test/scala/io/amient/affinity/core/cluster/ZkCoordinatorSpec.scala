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

package io.amient.affinity.core.cluster

import com.typesafe.config.ConfigValueFactory
import io.amient.affinity.AffinityActorSystem
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.actor.{GatewayHttp, Routed}
import io.amient.affinity.core.util.{AffinityTestBase, Reply}
import io.amient.affinity.kafka.EmbeddedZooKeeper
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

case class ZkTestKey(val id: Int) extends AvroRecord with Routed with Reply[Option[ZkTestValue]] {
  override def key = this
}

case class ZkTestValue(items: List[Int]) extends AvroRecord

class ZkCoordinatorSpec extends FlatSpec with AffinityTestBase with EmbeddedZooKeeper with Matchers {

  def config = configure("distributedit", zkConnect = Some(zkConnect))

  val system = AffinityActorSystem.create("test", config)
  val node1 = new Node(config)
  node1.startGateway(new GatewayHttp {

    val regionService = keyspace("region")

    //FIXME
    //    import MyTestPartition._
    //    import context.dispatcher
    //
    //    implicit val scheduler = context.system.scheduler
    //
    //    override def handle: Receive = {
    //      case HTTP(GET, PATH(key), _, response) =>
    //        implicit val timeout = Timeout(500 milliseconds)
    //        delegateAndHandleErrors(response, keyspace1 ack GetValue(key)) {
    //          _ match {
    //            case None => HttpResponse(NotFound)
    //            case Some(value) => Encoder.json(OK, value, gzip = false)
    //          }
    //        }
    //    }
  })

  val node2 = new Node(config.withValue("affinity.node.container.region",
    ConfigValueFactory.fromIterable(List(0, 1, 2, 3))))

  //  val region2 = new Node(config.withValue("affinity.node.container.region",
  //    ConfigValueFactory.fromIterable(List(1,3,5,7))))


  override protected def beforeAll(): Unit = try {
    node2.start()
    //    region2.start()
    node1.awaitClusterReady()
  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
    node1.shutdown()
    //      region2.shutdown()
    node2.shutdown()
  } finally {
    super.afterAll()
  }

  it should "do something that is not completely cear" in {

    //  try {
    //    val coordinator1 = Coordinator.create(system, "group1")
    //    val actor1 = system.actorOf(Props(new Actor {
    //      override def receive: Receive = {
    //        case null =>
    //      }
    //    }), "actor1")
    //    coordinator1.register(actor1.path)
    //    val update1 = new AtomicReference[String]("")
    //    update1 synchronized {
    //      coordinator1.watch(system.actorOf(Props(new Actor {
    //        override def receive: Receive = {
    //          case MasterStatusUpdate(g, add, del) => update1 synchronized update1.set(s"$g, ${add.size}, ${del.size}")
    //        }
    //      }), "subscriber1"), false)
    //    }
    //    coordinator1.close()
    //
    //    val coordinator2 = Coordinator.create(system, "group1")
    //    val update2 = new AtomicReference[String]("")
    //    update2 synchronized {
    //      coordinator2.watch(system.actorOf(Props(new Actor {
    //        override def receive: Receive = {
    //          case MasterStatusUpdate(g, add, del) => update2 synchronized update2.set(s"$g, ${add.size}, ${del.size}")
    //        }
    //      }), "subscriber2"), true)
    //      update2.wait(1000)
    //      update2.get should be("group1, 1, 0")
    //      update1.get should be("group1, 1, 0")
    //    }
    //    coordinator2.close()
    //
    //
    //  } finally {
    //    system.terminate()
    //  }
  }


}
