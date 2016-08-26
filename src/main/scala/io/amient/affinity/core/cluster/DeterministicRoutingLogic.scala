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

import java.util.concurrent.ConcurrentHashMap

import akka.routing.{ActorRefRoutee, Routee, RoutingLogic}

import scala.collection.immutable

case class DeterministicRoutingLogic(val numPartitions: Int) extends RoutingLogic {

  private var prevRoutees: immutable.IndexedSeq[Routee] = immutable.IndexedSeq()
  private val currentRouteMap = new ConcurrentHashMap[Int, Routee]()

  def partition(message: Any): Int = {
    (math.abs(message.hashCode()) match {
      case Int.MinValue => 0
      case a => a
    })  % numPartitions
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
    val p = partition(message)

    //TODO test the suspended scenario
    if (!currentRouteMap.containsKey(p)) throw new IllegalStateException(
      s"Partition `$p` is not represented by any Actor - " +
        s"this shouldn't happen - gateway should suspend all requests until all partitions are present")

    currentRouteMap.get(p)
  }

}
