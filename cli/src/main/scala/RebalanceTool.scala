import java.io.{File, PrintWriter}
import java.nio.file.Path

import com.typesafe.config.Config
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.util.{ZkClients, ZkConf}
import org.I0Itec.zkclient.ZkClient
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConverters._

case class Assignment(topic: String, partition: Int, replicas: List[Int], target: List[Int]) {
  def modified: Boolean = replicas != target

  override def toString: String = {
    s"topic: $topic, partition: $partition, current-replicas: [${replicas.mkString(",")}]" +
      (if (modified) s", target-replicas: [${target.mkString(",")}]" else "")
  }
}

object RebalanceTool extends Tool {

  class RebalanceConf extends CfgStruct[RebalanceConf] {
    val Zookeeper = string("zookeeper", true).doc("connection to the zookeeper that coordinates the kafka cluster")
    val ReplicationFactor = integer("replication.factor", true).doc("desired replication factor after rebalance")
    //TODO val RemoveBrokers = intlist("remove.brokeres", false).doc("brokers to ")
    val IgnoreBroker = integer("ignore.broker", false).doc("broker to ignore in the assignment")
    val Topic = string("topic", false).doc("only reblance a specific topic")
    val ConfigFile = filepath("config.file", false).doc("kafka client properties file for additional settings like security")
    val OutputFile = filepath("output.file", false).doc("path to the .json file where the output assignment will be generated")
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
    val zkClient = ZkClients.get(zkConf)
    try {
      val topics = if (conf.Topic.isDefined) Some(conf.Topic()).toList else List.empty
      val ignoreBrokers: Set[Int] = if (conf.IgnoreBroker.isDefined) Set(conf.IgnoreBroker()) else Set.empty
      val jsonFile = if (conf.OutputFile.isDefined) Some(conf.OutputFile()) else None
      apply(zkClient, conf.ReplicationFactor(), topics, jsonFile, ignoreBrokers)
    } finally {
      ZkClients.close(zkClient)
    }

  }

  def apply(zkClient: ZkClient, targetReplFactor: Int, topicsOnly: List[String], jsonFile: Option[Path], ignoreBrokers: Set[Int]): Unit = {
    val mapper = new ObjectMapper()

    val brokers: List[Int] = zkClient.getChildren("/brokers/ids").asScala.map(_.toInt).toList
      .filter(b => !ignoreBrokers.contains(b)).sorted
    val numBrokers = brokers.length

    if (targetReplFactor < 1) throw new IllegalArgumentException(s"Target replication factor must be at least 1")

    println(s"Available Brokers: [${brokers.mkString(",")}]")
    println(s"Target replication factor: $targetReplFactor")
    println("---------------------------------------------------------------------------------------------------------")
    if (numBrokers < targetReplFactor) throw new IllegalArgumentException(
      s"Target replication factor $targetReplFactor is higher than number of available brokers: $numBrokers")

    val topics = zkClient.getChildren("/brokers/topics").asScala.filter(x => topicsOnly.isEmpty || topicsOnly.contains(x))
    val assignments = topics.flatMap { topic =>
      val data = mapper.readTree(zkClient.readData[String](s"/brokers/topics/$topic"))
      val partitions = data.get("partitions").getFieldNames.asScala.map(_.toInt).toList.sorted
      partitions.map { partition =>
        val target: List[Int] = (0 until targetReplFactor).toList.map(r => brokers((partition + r) % numBrokers))
        val replicas = data.get("partitions").get(partition.toString).getElements.asScala.map(_.asInt).toList
        Assignment(topic, partition, replicas, target)
      }
    }

    assignments.foreach {
      assignment =>
        if (assignment.modified) try {
          print(27.toChar + "[93m")
          println(assignment)
        } finally {
          print(27.toChar + "[0m")
        } else {
          println(assignment)
        }
    }
    println("---------------------------------------------------------------------------------------------------------")
    if (assignments.forall(!_.modified)) {
      try {
        print(27.toChar + "[92m")
        println("All topics are balanced")
      } finally print(27.toChar + "[0m")
    } else jsonFile.foreach { path =>
      try {
        print(27.toChar + "[93m")
        print(s"Generating file ${path}")
      } finally println(27.toChar + "[0m")
      val pw = new PrintWriter(path.toFile)
      try {
        val json = mapper.createObjectNode()
        json.put("version", "1")
        val jsonPartitions = json.putArray("partitions")
        assignments.filter(_.modified).foreach {
          assignment =>
            val jsonAssignment = jsonPartitions.addObject()
            jsonAssignment.put("topic", assignment.topic)
            jsonAssignment.put("partition", assignment.partition)
            val jsonReplicas = jsonAssignment.putArray("replicas")
            assignment.target.foreach(jsonReplicas.add)
        }
        pw.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json))

      } finally {
        pw.close()
      }
    }

  }

}
