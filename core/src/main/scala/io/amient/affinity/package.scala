package io.amient

import com.typesafe.config.Config
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.actor.Keyspace.KeyspaceConf
import io.amient.affinity.core.cluster.Coordinator
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
    val Hostname: CfgString = string("remote.netty.tcp.hostname", false)
    val Port: CfgInt = integer("remote.netty.tcp.port", false)
  }

  class AffinityConf extends CfgStruct[AffinityConf] {
    val Avro: AvroConf = struct("avro", new AvroConf(), true)
    val Coorinator: Coordinator.CoorinatorConf = struct("coordinator", new Coordinator.CoorinatorConf, true)
    val Keyspace: CfgGroup[KeyspaceConf] = group("keyspace", classOf[KeyspaceConf], false)
    val Global: CfgGroup[StateConf] = group("global", classOf[StateConf], false)
    val Node = struct("node", new NodeConf, true)
  }

}
