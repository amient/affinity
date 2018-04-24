import com.typesafe.config.Config
import io.amient.affinity
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.LocalSchemaRegistry.LocalAvroConf
import io.amient.affinity.avro.MemorySchemaRegistry.MemAvroConf
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.avro.{ConfluentSchemaRegistry, LocalSchemaRegistry, MemorySchemaRegistry, ZookeeperSchemaRegistry}
import io.amient.affinity.core.cluster.CoordinatorEmbedded.EmbedConf
import io.amient.affinity.core.cluster.CoordinatorZk.CoordinatorZkConf
import io.amient.affinity.core.cluster.{CoordinatorEmbedded, CoordinatorZk}
import io.amient.affinity.core.config.{Cfg, CfgGroup, CfgStruct}
import io.amient.affinity.core.storage.rocksdb.MemStoreRocksDb
import io.amient.affinity.kafka.KafkaLogStorage
import io.amient.affinity.kafka.KafkaStorage.{KafkaStateConf, KafkaStorageConf}

import scala.collection.JavaConverters._

object DocTool extends Tool {

  class DocToolConf extends CfgStruct[DocToolConf] {
    doc("Generating Affinity Configuration documentation")
  }

  object Conf extends DocToolConf {
    override def apply(config: Config) = new DocToolConf().apply(config)
  }

  def apply(config: Config): Unit = {
    println("\n\n## Avro")
    apply(affinity.Conf.Affi.Avro)

    println(s"\n### Avro (${classOf[ConfluentSchemaRegistry].getName})")
    apply(CfAvroConf(affinity.Conf.Affi.Avro))

    println(s"\n### Avro (${classOf[ZookeeperSchemaRegistry].getName})")
    apply(ZkAvroConf(affinity.Conf.Affi.Avro))

    println(s"\n### Avro (${classOf[LocalSchemaRegistry].getName})")
    apply(LocalAvroConf(affinity.Conf.Affi.Avro))

    println(s"\n### Avro (${classOf[MemorySchemaRegistry].getName})")
    apply(MemAvroConf(affinity.Conf.Affi.Avro))

    println("\n\n## Coordinator")
    apply(affinity.Conf.Affi.Coordinator)

    println(s"\n### Coordinator (${classOf[CoordinatorZk].getName})")
    apply(CoordinatorZkConf(affinity.Conf.Affi.Coordinator))

    println(s"\n### Coordinator (${classOf[CoordinatorEmbedded].getName})")
    apply(EmbedConf(affinity.Conf.Affi.Coordinator))

    println("\n\n## Global State")
    apply(affinity.Conf.Affi.Global)

    println(s"\n### Global State Storage(${classOf[KafkaLogStorage].getName})")
    apply(new KafkaStateConf().apply(affinity.Conf.Affi.Global("<ID>")))

    println(s"\n### Global State Memstore(${classOf[MemStoreRocksDb].getName})")
    apply(new MemStoreRocksDb.MemStoreRocksDbConf().apply(affinity.Conf.Affi.Global("<ID>")))

    println("\n\n## Keyspaces")
    apply(affinity.Conf.Affi.Keyspace)

    println(s"\n### Keyspaces Storage(${classOf[KafkaLogStorage].getName})")
    apply(new KafkaStateConf().apply(affinity.Conf.Affi.Keyspace("<ID>").State("<ID>")))

    println(s"\n### Keyspaces Memstore(${classOf[MemStoreRocksDb].getName})")
    apply(new MemStoreRocksDb.MemStoreRocksDbConf().apply(affinity.Conf.Affi.Keyspace("<ID>").State("<ID>")))


    println("\n\n## Node Context")
    apply(affinity.Conf.Affi.Node)

    println(s"\n### Node Context Stream(${classOf[KafkaLogStorage].getName})")
    apply(new KafkaStorageConf().apply(affinity.Conf.Affi.Node.Gateway.Stream("<ID>")))

    println("\n\n## Important Akka Configuration Options")
    apply(affinity.Conf.Akka)

  }

  def apply(cfg: Cfg[_]): Unit = {

    def printInfo(cfg: Cfg[_]): Unit = {
      val defaultInfo = if (cfg.isDefined) cfg.defaultInfo() else if (cfg.isRequired) "!" else "-"
      val configInfo = s"${cfg.path} ${if (cfg.parameterInfo.isEmpty) "" else "[" + cfg.parameterInfo.toUpperCase +"] ("+ defaultInfo +") "}"
      println(s"\t${configInfo.padTo(80, ' ')}\t${cfg.description}")
    }

    cfg match {
      case struct: CfgStruct[_] =>
        if (!struct.description().isEmpty) {
          printInfo(struct)
        }
        struct.map.values().asScala.foreach(item => apply(item))
//        if (struct.options.contains(Cfg.Options.IGNORE_UNKNOWN)) {
//          println(s"\t${struct.path}.*")
//        }
      case group: CfgGroup[_] =>
        printInfo(cfg)
        val g = group.getGroupClass.newInstance()
        g.setPath(group.path("<ID>"))
        apply(g)
      case _ => printInfo(cfg)
    }

  }

}
