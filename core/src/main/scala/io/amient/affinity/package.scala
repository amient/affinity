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

package io.amient

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.actor.Keyspace.KeyspaceConf
import io.amient.affinity.core.cluster.Coordinator.CoorinatorConf
import io.amient.affinity.core.cluster.Node.NodeConf
import io.amient.affinity.core.config._
import io.amient.affinity.core.storage.StateConf

package object affinity {

  object Conf extends Conf {
    override def apply(config: Config): Conf = new Conf().apply(config)
  }

  class Conf extends CfgStruct[Conf](Cfg.Options.IGNORE_UNKNOWN) {
    val Akka: AkkaConf = struct("akka", new AkkaConf, false)
    val Affi: AffinityConf = struct("affinity", new AffinityConf, true)
  }

  class AkkaConf extends CfgStruct[AkkaConf](Cfg.Options.IGNORE_UNKNOWN) {
    //TODO if Remote Hostname and Port are configured then other remote settings should be auto-configured, e.g. actor provider, enabled-transports
    val Hostname: CfgString = string("remote.netty.tcp.hostname", false)
    val Port: CfgInt = integer("remote.netty.tcp.port", false)
    val RemoteTransports = stringlist("remote.enabled-transports", false).doc("Set this to [\"akka.remote.netty.tcp\"] when running in a cluster")
    val ActorProvider = string("actor.provider", false).doc("Set this to \"akka.remote.RemoteActorRefProvider\" when running in a cluster")
    //TODO the following ones should be set on the application config objects if not present
    val HttpServerIdleTimeout = string("http.server.idle-timeout", "infinite")
    val HttpServerRequestTimeout = string("http.server.request-timeout", "30s")
    val HttpServerMaxConnections = integer("http.server.max-connections", 1000)
    val HttpServerRemoteAddressHeader = string("http.server.remote-address-header", "on")
    val HttpServerHeaderDiscolsure = string("http.server.server-header", "-")
  }

  class AffinityConf extends CfgStruct[AffinityConf] {
    val Avro: AvroConf = struct("avro", new AvroConf(), true)
    val Coordinator: CoorinatorConf = struct("coordinator", new CoorinatorConf, true)
    val Keyspace: CfgGroup[KeyspaceConf] = group("keyspace", classOf[KeyspaceConf], false)
    val Global = group("global", classOf[StateConf], false).doc("each global state has an ID and needs to be further configured")
    val Node = struct("node", new NodeConf, true)
  }

  object AffinityActorSystem {
    val defaultConfig = ConfigFactory.parseResources(getClass.getClassLoader, "affinity.conf")
    require(defaultConfig.hasPath("akka.actor.serializers.avro"))
    def apply(config: Config): Config = config.withFallback(defaultConfig)
    def create(name: String, config: Config): ActorSystem = ActorSystem.create(name, apply(config))
  }

}
