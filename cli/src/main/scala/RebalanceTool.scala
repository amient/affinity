import com.typesafe.config.Config
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.util.{ZkClients, ZkConf}
import org.I0Itec.zkclient.ZkClient

import scala.collection.JavaConverters._



object RebalanceTool extends Tool {

  class RebalanceConf extends CfgStruct[RebalanceConf] {
    val Zookeeper = string("zookeeper", true).doc("connection to the zookeeper that coordinates the kafka cluster")
    val ConfigFile = filepath("config.file", false).doc("kafka client properties file for additional settings like security")
    doc("Tool for rebalancing partition replicas and leaders across kafka cluster")
  }

  object Conf extends RebalanceConf {
    override def apply(config: Config) = new RebalanceConf().apply(config)
  }

  def printHelp(): Unit = {
    Conf.map().asScala.foreach(println)
    println("Usage: rebalance --bootstrap-server <kafka-bootstrap-server> [--config-file <kafka-client-properties-file>]\n")
    sys.exit(1)
  }


  def apply(config: Config) = apply(Conf(config))

  def apply(conf: RebalanceConf): Unit = {
    val zkConf = new ZkConf()
    zkConf.Connect.setValue(conf.Zookeeper())
    apply(ZkClients.get(zkConf))
  }

  def apply(zkClient: ZkClient): Unit = {
    try {
      println("1")
    } finally {
      ZkClients.close(zkClient)
    }
  }

}
