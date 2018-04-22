import java.io.FileInputStream
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.config.CfgStruct
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object RebalanceTool extends Tool {

  class RebalancerConf extends CfgStruct[RebalancerConf] {
    val BootstrapServer = string("bootstrap.server", true).doc("kafka bootstrap server")
    val ConfigFile = filepath("config.file", false).doc("kafka client properties file for additional settings like security")
    doc("Tool for rebalancing partition replicas and leaders across kafka cluster")
  }

  object Conf extends RebalancerConf {
    override def apply(config: Config) = new RebalancerConf().apply(config)
  }

  def printHelp(): Unit = {
    Conf.map().asScala.foreach(println)
    println("Usage: rebalance --bootstrap-server <kafka-bootstrap-server> [--config-file <kafka-client-properties-file>]\n")
    sys.exit(1)
  }

  def apply(args: List[String], config: Config): Unit = args match {
    case "--bootstarp-server" :: bootstrapServer :: tail => apply(tail, config.withValue("bootstrap.server", ConfigValueFactory.fromAnyRef(bootstrapServer)))
    case "--config-file" :: configFile :: tail => apply(tail, config.withValue("config.file", ConfigValueFactory.fromAnyRef(configFile)))
    case Nil => try apply(new RebalancerConf().apply(config)) catch {
      case NonFatal(e) =>
        println(e.getMessage)
        printHelp()
    }
    case _ => printHelp()
  }

  def apply(conf: RebalancerConf): Unit = {
    val adminProps = new Properties()
    if (conf.ConfigFile.isDefined) {
      val is = new FileInputStream(conf.ConfigFile().toFile)
      try adminProps.load(is) finally is.close
    }
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, conf.BootstrapServer())
    apply(adminProps)
  }

  def apply(adminProps: Properties): Unit = {
    val admin = AdminClient.create(adminProps)
    try {
      val adminTimeoutMs: Long = 15000
    } finally {
      admin.close()
    }
  }

}
