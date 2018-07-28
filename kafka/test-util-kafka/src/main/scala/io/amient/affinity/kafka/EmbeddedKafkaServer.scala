package io.amient.affinity.kafka

import java.io.File
import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.server.{KafkaConfig, KafkaServerStartable}
import kafka.zk.BrokerIdZNode
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.commons.compress.utils.Charsets
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

trait EmbeddedKafkaServer extends EmbeddedService with EmbeddedZookeperServer {

  private val log = LoggerFactory.getLogger(classOf[EmbeddedKafka])

  def numPartitions: Int

  private val embeddedKafkaPath = new File(testDir, "local-kafka-logs")
  private val kafkaConfig = new KafkaConfig(new Properties {
    put("broker.id", "1")
    put("host.name", "localhost")
    put("port", "0")
    put("log.dir", embeddedKafkaPath.toString)
    put("num.partitions", numPartitions.toString)
    put("auto.create.topics.enable", "false")
    put("delete.topic.enable", "true")
    put("zookeeper.connect", zkConnect)
    put("offsets.topic.replication.factor", "1")
  })

  private val kafka = new KafkaServerStartable(kafkaConfig)
  kafka.startup()

  lazy val admin = AdminClient.create(Map[String, AnyRef]("bootstrap.servers" -> kafkaBootstrap).asJava)

  def createTopic(name: String): Unit = {
    admin.createTopics(List(new NewTopic(name, numPartitions, 1)).asJava).all().get(30, TimeUnit.SECONDS)
  }

  def listTopics: mutable.Set[String] = {
    admin.listTopics().names().get(1, TimeUnit.SECONDS).asScala
  }

  val tmpZkClient = new ZkClient(zkConnect, 5000, 6000, new ZkSerializer {
    def serialize(o: Object): Array[Byte] = o.toString.getBytes

    override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
  })

  val broker = BrokerIdZNode.decode(1, tmpZkClient.readData[String]("/brokers/ids/1").getBytes(Charsets.UTF_8)).broker
  val kafkaBootstrap = broker.brokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)).connectionString()
  tmpZkClient.close
  log.info(s"Embedded Kafka $kafkaBootstrap, data dir: $testDir")

  abstract override def close(): Unit = try kafka.shutdown finally super.close

}
