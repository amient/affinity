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

import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import akka.dispatch.Dispatchers
import akka.event.Logging
import akka.routing._
import io.amient.affinity.core.actor.Partition.Keyed

import scala.collection.immutable

final case class PartitionedGroup(numPartitions: Int) extends Group {

  override def paths(system: ActorSystem) = List()

  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId

  override def createRouter(system: ActorSystem): Router = {
    val log = Logging.getLogger(system, this)
    log.info("Creating PartitionedGroup router")

    val defaultLogic = RoundRobinRoutingLogic()

    val partitioningLogic = new RoutingLogic {

      private var prevRoutees: immutable.IndexedSeq[Routee] = immutable.IndexedSeq()
      private val currentRouteMap = new ConcurrentHashMap[Int, Routee]()

      def abs(i: Int): Int = math.abs(i) match {
        case Int.MinValue => 0
        case a => a
      }

      def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {

        if (!prevRoutees.eq(routees)) {
          prevRoutees.synchronized {
            if (!prevRoutees.eq(routees)) {
              currentRouteMap.clear()
              routees.foreach {
                case actorRefRoutee: ActorRefRoutee =>
                  currentRouteMap.put(actorRefRoutee.ref.path.name.split("-")(1).toInt, actorRefRoutee)
              }
              prevRoutees = routees
            }
          }
        }
        val partition = abs(message.hashCode()) % numPartitions

        //TODO test the suspended scenario
        if (!currentRouteMap.containsKey(partition)) throw new IllegalStateException(
          s"Partition `$partition` is not represented by any Actor - " +
            s"this shouldn't happen - gateway should suspend all requests until all partitions are present")

        currentRouteMap.get(partition)
      }

    }

    new Router(new RoutingLogic {
      def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
        message match {
          case k: Keyed[_] => partitioningLogic.select(message, routees)
          case _ => defaultLogic.select(message, routees)
        }
      }
    })
  }

}