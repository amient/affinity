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

package io.amient.affinity.example.actor

import akka.actor.{Actor, Status}

import scala.io.StdIn


class UserInputMediator extends Actor {

  override def receive: Receive = {
    case greeting: String =>
      require(greeting != null && !greeting.isEmpty, "User Mediator requires non-empty greeting")
      print(s"'$greeting', please reply: ")
      //IOException on the next line may result in this actor being restarted
      sender ! StdIn.readLine()

    case _ =>
      //not a fault of this actor, sender's bad
      sender ! Status.Failure(new IllegalArgumentException("UserInputMediator only understands String messages"))
  }

}
