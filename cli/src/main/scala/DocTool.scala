import com.typesafe.config.Config
import io.amient.affinity
import io.amient.affinity.core.config.{Cfg, CfgGroup, CfgStruct}

import scala.collection.JavaConverters._

object DocTool extends Tool {

  class DocToolConf extends CfgStruct[DocToolConf] {
    doc("Generating Affinity Configuration documentation")
  }

  object Conf extends DocToolConf {
    override def apply(config: Config) = new DocToolConf().apply(config)
  }

  override def apply(args: List[String], config: Config): Unit = apply(affinity.Conf)

  def apply(cfg: Cfg[_], extraPath: String = ""): Unit = {
    //TODO extenesions
    cfg match {
      case struct: CfgStruct[_] =>
        struct.map.values().asScala.foreach(item => apply(item, extraPath))
      case group: CfgGroup[_] =>
        println(s"${(extraPath + cfg.path()).padTo(70, ' ')}\t- ${cfg.description}")
        val g = group.getGroupClass.newInstance()
//        g.path(cfg.path())
        apply(g, extraPath + s"${cfg.path()}.<ID>.")
      case other =>
        println(s"${(extraPath + cfg.path()).padTo(70, ' ')}\t- ${cfg.description}")
    }
  }

}
