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

import java.util.concurrent.atomic.AtomicInteger
import java.util.{Observable, Observer}

import akka.actor.{ActorPath, ActorSystem}
import com.typesafe.config.Config

import scala.collection.mutable

object CoordinatorEmbedded extends Observable {

  final val CONFIG_TEST_COORDINATOR_ID = "affinity.cluster.coordinator.id"
  final val AUTO_COORDINATOR_ID = new AtomicInteger(1000000)

  private val services = mutable.Map[String, mutable.Map[String, String]]()

  def get(space: String): Map[String, String] = synchronized {
    services.get(space) match {
      case None => Map()
      case Some(g) => g.toMap
    }
  }

  def remove(space: String, handle: String) = update(space) { g =>
    g -= handle
  }

  def put(space: String, handle: String, handle1: String) = update(space) { g =>
    g += handle -> handle
    setChanged()
    notifyObservers((space, services(space).toMap))
  }

  private def update(group: String)(f: mutable.Map[String, String] => Unit) = synchronized {
    if (!services.contains(group)) services += group -> mutable.Map()
    f(services(group))
  }

  override def addObserver(o: Observer): Unit = {
    super.addObserver(o)
    services.foreach { case (space, mapping) =>
      o.update(this, (space, mapping.toMap))
    }
  }

}

class CoordinatorEmbedded(system: ActorSystem, group: String, config: Config) extends Coordinator(system, group) with Observer {

  val id = system.settings.config.getInt(CoordinatorEmbedded.CONFIG_TEST_COORDINATOR_ID)
  val space = s"$id:$group"

  CoordinatorEmbedded.addObserver(this)

  def services: Map[String, String] = CoordinatorEmbedded.get(space)

  override def register(actorPath: ActorPath): String = {
    val handle = actorPath.toString
    CoordinatorEmbedded.put(space, handle, handle)
    handle
  }

  override def unregister(handle: String): Unit = {
    CoordinatorEmbedded.remove(space, handle)
  }

  override def update(o: Observable, arg: scala.Any): Unit = {
    arg.asInstanceOf[(String, Map[String, String])] match {
      case (s, services) if (s == space) => {
        if (!closed.get) {
          updateGroup(services)
        }
      }
      case _ =>
    }
  }
}