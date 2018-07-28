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
import io.amient.affinity.Conf
import io.amient.affinity.core.cluster.Coordinator.CoorinatorConf
import io.amient.affinity.core.cluster.CoordinatorEmbedded.{EmbedConf, groups}
import io.amient.affinity.core.config.CfgStruct

import scala.collection.mutable

object CoordinatorEmbedded {

  final val AutoCoordinatorId = new AtomicInteger(1000000)

  object EmbedConf extends EmbedConf {
    override def apply(config: Config): EmbedConf = new EmbedConf().apply(config)
  }

  class EmbedConf extends CfgStruct[EmbedConf](classOf[CoorinatorConf]) {
    val ID = integer("embedded.id", true).doc("embedded coordinator instances must have the same id to work together")
  }

  private val groups = mutable.Map[String, CoordinatedGroup]()

  private def get(group: String, observer: Observer): CoordinatedGroup = {
    (synchronized(if (!groups.contains(group)) {
      val g = new CoordinatedGroup
      groups += group -> g
      g
    } else groups(group))) match {
      case g => g.addObserver(observer); g
    }
  }

  private class CoordinatedGroup() extends Observable {
    private val members = mutable.Map[String, String]()

    override def addObserver(o: Observer): Unit = {
      super.addObserver(o)
      o.update(this, get)
    }

    def remove(handle: String) = {
      synchronized(members -= handle)
      setChanged()
      notifyObservers(get)
    }

    def put(handle: String) = {
      synchronized(members += handle -> handle)
      setChanged()
      notifyObservers(get)
    }

    def get = synchronized(members.toMap)

  }

}


class CoordinatorEmbedded(system: ActorSystem, group: String, config: Config) extends Coordinator(system, group) with Observer {

  val conf = EmbedConf(Conf(system.settings.config).Affi.Coordinator)
  val id = conf.ID()
  private val realm = CoordinatorEmbedded.get(s"$id:$group", this)

  def members: Map[String, String] = realm.get

  override def register(actorPath: ActorPath): String = {
    val handle = actorPath.toString
    realm.put(handle)
    handle
  }

  override def unregister(handle: String): Unit = realm.remove(handle)

  override def update(o: Observable, arg: scala.Any): Unit = updateGroup(arg.asInstanceOf[Map[String, String]])
}
