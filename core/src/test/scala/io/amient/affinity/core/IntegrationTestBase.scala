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

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.actor.Cluster.ClusterAvailability
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

class IntegrationTestBase(system: ActorSystem) extends TestKit(system) with ImplicitSender with WordSpecLike with BeforeAndAfterAll {

  def this() = this(ActorSystem.create("AffinitySpec",ConfigFactory.load("integrationtests")))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def awaitClusterReady() {
    val clusterReady = new AtomicBoolean(false)
    system.eventStream.subscribe(system.actorOf(Props(new Actor {
      override def receive: Receive = {
        case ClusterAvailability(false) => {
          clusterReady.set(true)
          clusterReady.synchronized(clusterReady.notify)
        }
      }
    })), classOf[ClusterAvailability])
    clusterReady.synchronized(clusterReady.wait(15000))
    assert(clusterReady.get)
  }

}
