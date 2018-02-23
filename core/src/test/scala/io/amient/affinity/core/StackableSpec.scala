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

package io.amient.affinity.core

import akka.actor.Actor.Receive
import org.slf4j.LoggerFactory


object StackableSpec extends App {

  val gate = new Gate with Module1 with Module2
  gate.handle.apply("HI")
  gate.handle.apply(100)
}

abstract class Gate {
  def handle: Receive = {
    case null =>
  }
}

trait Module1 extends Gate {

  val sharedData = "shared-data"

  private val log = LoggerFactory.getLogger(classOf[Module1])

  abstract override def handle: Receive = super.handle orElse {
    case in: Int => log.info(s"Handled by Module1: " + in)
  }
}

trait Module2 extends Gate {
  self: Module1 => //this module requries another module

  private val log = LoggerFactory.getLogger(classOf[Module2])

  abstract override def handle: Receive = super.handle orElse {
    case in: String => log.info(s"Handled by Module2: " + in +s" using $sharedData")
  }
}







