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

package io.amient.affinity.core.actor

import akka.actor.{ActorPath, ActorSystem, PoisonPill, Props}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.cluster.CoordinatorEmbedded.EmbedConf
import io.amient.affinity.core.cluster.{Coordinator, CoordinatorEmbedded}
import io.amient.affinity.{AffinityActorSystem, Conf}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps


class RegionSpec extends WordSpecLike with Matchers with Eventually with IntegrationPatience {

  val testPartition = Props(new Partition {
    override def preStart(): Unit = {
      Thread.sleep(100)
      super.preStart()
    }

    override def handle: Receive = {
      case e: IllegalStateException => context.stop(self)
      case _ =>
    }
  })

  "A Region Actor" must {
    "must keep Coordinator Updated during partition failure & restart scenario" in {
//      val zk = new EmbeddedZookeperServer {}
      try {
        val system: ActorSystem = AffinityActorSystem.create("IntegrationTestSystem",
          ConfigFactory.load("integrationtests")
//            .withValue(Conf.Affi.Coordinator.Class.path, ConfigValueFactory.fromAnyRef(classOf[CoordinatorZk].getName))
//            .withValue(CoordinatorZkConf(Conf.Affi.Coordinator).ZooKeeper.Connect.path,
//              ConfigValueFactory.fromAnyRef(zk.zkConnect))
            .withValue(EmbedConf(Conf.Affi.Coordinator).ID.path,
              ConfigValueFactory.fromAnyRef(CoordinatorEmbedded.AutoCoordinatorId.incrementAndGet()))
        )
        val coordinator = Coordinator.create(system, "region")
        try {
          val d = 1 second
          implicit val timeout = Timeout(d)

          val region = system.actorOf(Props(new Container("region") {
            val partitions = List(0, 1, 2, 3)
            for (partition <- partitions) {
              context.actorOf(testPartition, name = partition.toString)
            }
          }), name = "region")
          eventually {
            coordinator.members.size should be(4)
          }

          //first stop Partition explicitly - it shouldn't be restarted
          import system.dispatcher
          system.actorSelection(ActorPath.fromString(coordinator.members.head._2)).resolveOne.foreach {
            case actorRef => system.stop(actorRef)
          }
          eventually {
            coordinator.members.size should be(3)
          }

          //now simulate error in one of the partitions
          val partitionToFail = coordinator.members.head._2
          system.actorSelection(ActorPath.fromString(partitionToFail)).resolveOne.foreach {
            case actorRef => actorRef ! new IllegalStateException("Exception expected by the Test")
          }
          eventually {
            coordinator.members.size should be(2)
          }
          eventually {
            coordinator.members should not contain (partitionToFail)
          }

          region ! PoisonPill

        } finally {
          coordinator.close
        }
      } finally {
//        zk.close()
      }
    }
  }

}
