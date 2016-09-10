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

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.actor.Controller.{CreateGateway, CreateRegion, CreateServiceContainer}
import io.amient.affinity.core.actor._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

class Node(appConfig: Properties) {

  val akkaPort = appConfig.getProperty(Container.CONFIG_AKKA_PORT)
  val actorSystemName = appConfig.getProperty(Container.CONFIG_AKKA_SYSTEM)

  val systemConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort")
    .withFallback(ConfigFactory.load("example"))

  implicit val system = ActorSystem(actorSystemName, systemConfig)

  private val controller = system.actorOf(Props(new Controller(appConfig)), name = "controller")

  //in case the process is stopped from outside
  sys.addShutdownHook {
    system.terminate()
    //we cannot use the future returned by system.terminate() because shutdown may have already been invoked
    Await.ready(system.whenTerminated, 30 seconds) // TODO shutdown timeout by configuration
  }

  def startGateway[T <: Gateway](creator: => T)(implicit tag: ClassTag[T]): Unit = {
    controller ! CreateGateway(Props(creator))
  }

  def startRegion[T <: Partition](partitionCreator: => T)(implicit tag: ClassTag[T]) = {
    controller ! CreateRegion(Props(partitionCreator))
  }

  def startServices(services: Props*) = {
    require(services.forall(props => classOf[Service].isAssignableFrom(props.actorClass)))
    controller ! CreateServiceContainer(services)
  }

  implicit def serviceCreatorToProps[T <: Service](creator: => T)(implicit tag: ClassTag[T]): Props = {
    Props(creator)
  }



}
