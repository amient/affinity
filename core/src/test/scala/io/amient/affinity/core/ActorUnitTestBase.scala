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

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.Region
import io.amient.affinity.core.cluster.Node
import org.scalatest.WordSpecLike

import scala.collection.JavaConverters._

class ActorUnitTestBase(system: ActorSystem) extends TestKit(system) with ImplicitSender with WordSpecLike {

  def this(config: Config) = this(ActorSystem.create("AffinitySpec", config))

  def this() = this(ConfigFactory.defaultApplication()
    .withValue(Node.CONFIG_AKKA_HOST, ConfigValueFactory.fromAnyRef(null))
    .withValue(Region.CONFIG_PARTITION_LIST, ConfigValueFactory.fromIterable(List(0, 1, 2, 3).asJava))
  )

}
