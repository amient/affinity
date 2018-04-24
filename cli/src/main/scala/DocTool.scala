import com.typesafe.config.Config
import io.amient.affinity
import io.amient.affinity.avro.{ConfluentSchemaRegistry, MemorySchemaRegistry, ZookeeperSchemaRegistry}
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.MemorySchemaRegistry.MemAvroConf
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.core.cluster.CoordinatorEmbedded.EmbedConf
import io.amient.affinity.core.cluster.{CoordinatorEmbedded, CoordinatorZk}
import io.amient.affinity.core.cluster.CoordinatorZk.CoordinatorZkConf
import io.amient.affinity.core.config.{Cfg, CfgGroup, CfgStruct}

import scala.collection.JavaConverters._

object DocTool extends Tool {

  class DocToolConf extends CfgStruct[DocToolConf] {
    doc("Generating Affinity Configuration documentation")
  }

  object Conf extends DocToolConf {
    override def apply(config: Config) = new DocToolConf().apply(config)
  }

  def apply(config: Config): Unit = {
    println("\n\nAvro")
    println("------------------------------------------------------------------------------------------------------")
    apply(affinity.Conf.Affi.Avro)

    println(s"\nAvro (${classOf[ConfluentSchemaRegistry].getName})")
    println("------------------------------------------------------------------------------------------------------")
    apply(CfAvroConf(affinity.Conf.Affi.Avro))

    println(s"\nAvro (${classOf[ZookeeperSchemaRegistry].getName})")
    println("------------------------------------------------------------------------------------------------------")
    apply(ZkAvroConf(affinity.Conf.Affi.Avro))

    println(s"\nAvro (${classOf[MemorySchemaRegistry].getName})")
    println("------------------------------------------------------------------------------------------------------")
    apply(MemAvroConf(affinity.Conf.Affi.Avro))

    println("\n\nCoordinator")
    println("------------------------------------------------------------------------------------------------------")
    apply(affinity.Conf.Affi.Coordinator)

    println(s"\nCoordinator (${classOf[CoordinatorZk].getName})")
    println("------------------------------------------------------------------------------------------------------")
    apply(CoordinatorZkConf(affinity.Conf.Affi.Coordinator))

    println(s"\nCoordinator (${classOf[CoordinatorEmbedded].getName})")
    println("------------------------------------------------------------------------------------------------------")
    apply(EmbedConf(affinity.Conf.Affi.Coordinator))

    println("\n\nGlobal Key-Value Store Defintions")
    println("------------------------------------------------------------------------------------------------------")
    apply(affinity.Conf.Affi.Global)
    //TODO storage extenesions

    println("\n\nKey-Space Defintitions")
    println("------------------------------------------------------------------------------------------------------")
    apply(affinity.Conf.Affi.Keyspace)
    //TODO storage extenesions
    //TODO memstore extensions

    println("\n\nNode Deployment")
    println("------------------------------------------------------------------------------------------------------")
    apply(affinity.Conf.Affi.Node)
    println()
    //TODO stream storage extenesions

    println("\n\nImportant Akka Configuration Options")
    println("------------------------------------------------------------------------------------------------------")
    apply(affinity.Conf.Akka)
    println("\t.. all other akka settings may be applied")

  }

  def apply(cfg: Cfg[_]): Unit = {

    def printInfo(cfg: Cfg[_]): Unit = {
      val requiredInfo = (if (cfg.isRequired) "*" else "").padTo(2, ' ')
      val defaultInfo = (if (cfg.isDefined) cfg.defaultInfo() + " " else "") + "- "
      val paramterInfo = cfg.parameterInfo.padTo(10, ' ')
      println(s"\t${cfg.path.padTo(60, ' ')}\t$requiredInfo\t$paramterInfo\t$defaultInfo\t${cfg.description}")
    }

    cfg match {
      case struct: CfgStruct[_] => struct.map.values().asScala.foreach(item => apply(item))
      case group: CfgGroup[_] =>
        printInfo(cfg)
        val g = group.getGroupClass.newInstance()
        g.setPath(group.path("<ID>"))
        apply(g)
      case _ => printInfo(cfg)
    }

  }

}
