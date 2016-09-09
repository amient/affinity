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

package io.amient.affinity.example.rest

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.HttpInterface
import io.amient.affinity.core.actor.Controller.{CreateGateway, CreateRegion}
import io.amient.affinity.core.actor.{Controller, Node, Region}
import io.amient.affinity.core.cluster.{Cluster, Coordinator, ZkCoordinator}
import io.amient.affinity.example.rest.handler._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

object ApiNode extends App {

  try {
    require(args.length >= 6)

    val actorSystemName = args(0)
    val akkaPort = args(1).toInt
    val host = args(2)
    val httpPort = args(3).toInt
    val numPartitions = args(4).toInt
    val partitionList = args(5) // coma-separated list of partitions assigned to this node
    val zkConnect = if (args.length > 6) args(6) else "localhost:2181"

    val appConfig = new Properties()
    appConfig.put(HttpInterface.CONFIG_HTTP_HOST, host)
    appConfig.put(HttpInterface.CONFIG_HTTP_PORT, httpPort.toString)
    appConfig.put(Cluster.CONFIG_NUM_PARTITIONS, numPartitions.toString)
    appConfig.put(Node.CONFIG_AKKA_HOST, host)
    appConfig.put(Node.CONFIG_AKKA_PORT, akkaPort.toString)
    appConfig.put(Region.CONFIG_PARTITION_LIST, partitionList)
    appConfig.put(Coordinator.CONFIG_COORDINATOR_CLASS, classOf[ZkCoordinator].getName)
    appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_CONNECT, zkConnect)

    val systemConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort")
      .withFallback(ConfigFactory.load("example"))

    implicit val system = ActorSystem(actorSystemName, systemConfig)

    val controller = system.actorOf(Props(new Controller(appConfig)), name = "controller")

    //this cluster is symmetric - all nodes serve both as Gateways and Regions
    controller ! CreateGateway(Props(new HttpGateway(appConfig)
      with Describe
      with Ping
      with Fail
      with Connect
      with Access
    ))
    controller ! CreateRegion(Props(new ApiPartition(appConfig)))

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

