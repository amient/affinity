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

package io.amient.affinity.example.service

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.actor.Node
import io.amient.affinity.core.cluster.{Coordinator, ZkCoordinator}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

object ServiceNode extends App {

  try {
    require(args.length >= 3, "Service Node requires 3 argument: <akka-system-name>, <akka-host>, <akka-port>")

    val actorSystemName = args(0)
    val akkaPort = args(1).toInt
    val host = args(2)
    val zkConnect = if (args.length > 3) args(3) else "localhost:2181"

    val appConfig = new Properties()
    appConfig.put(Node.CONFIG_AKKA_HOST, host)
    appConfig.put(Node.CONFIG_AKKA_PORT, akkaPort.toString)
    appConfig.put(Coordinator.CONFIG_COORDINATOR_CLASS, classOf[ZkCoordinator].getName)
    appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_CONNECT, zkConnect)

    val systemConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort")
      .withFallback(ConfigFactory.load("example"))


    implicit val system = ActorSystem(actorSystemName, systemConfig)

    import system.dispatcher

    val coordinator = try {
      Coordinator.fromProperties(appConfig)
    } catch {
      case e: Throwable =>
        system.terminate() onComplete { _ =>
          e.printStackTrace()
          System.exit(10)
        }
        throw e
    }

    //TODO the fact that the node actor has a name "services" should be hidden behind the API - maybe refactor Controller to handle this as well
    val node = system.actorOf(Props(new Node(appConfig, coordinator, "services") {
      context.actorOf(Props(new UserInputMediator), name = classOf[UserInputMediator].getName)
    }), name = "services")

    //in case the process is stopped from outside
    sys.addShutdownHook {
      system.terminate()
      //we cannot use the future returned by system.terminate() because shutdown may have already been invoked
      Await.ready(system.whenTerminated, 30 seconds) // TODO shutdown timeout by configuration
    }

  } catch {
    case e: IllegalArgumentException =>
      e.printStackTrace()
      System.exit(1)
    case NonFatal(e) =>
      e.printStackTrace()
      System.exit(2)
    case e: Throwable =>
      e.printStackTrace()
      System.exit(3)
  }

}
