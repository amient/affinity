package io.amient.affinity.kafka

import java.io.File
import java.nio.file.Files
import java.util.Properties

import kafka.cluster.Broker
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

trait EmbeddedKafka extends EmbeddedZooKeeper with BeforeAndAfterAll {

  self: Suite =>

  private val log = LoggerFactory.getLogger(classOf[EmbeddedKafka])

  def numPartitions: Int

  private val testDir: File = Files.createTempDirectory(this.getClass.getSimpleName).toFile
  testDir.mkdirs()

  private val embeddedKafkaPath = new File(testDir, "local-kafka-logs")
  private val kafkaConfig = new KafkaConfig(new Properties {
    put("broker.id", "1")
    put("host.name", "localhost")
    put("port", "0")
    put("log.dir", embeddedKafkaPath.toString)
    put("num.partitions", numPartitions.toString)
    put("auto.create.topics.enable", "true")
    put("zookeeper.connect", zkConnect)
    put("offsets.topic.replication.factor", "1")
  })

  private val kafka = new KafkaServerStartable(kafkaConfig)
  kafka.startup()

  val tmpZkClient = new ZkClient(zkConnect, 5000, 6000, new ZkSerializer {
    def serialize(o: Object): Array[Byte] = o.toString.getBytes

    override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
  })

  val broker = Broker.createBroker(1, tmpZkClient.readData[String]("/brokers/ids/1"))
  val kafkaBootstrap = broker.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)).connectionString()
  tmpZkClient.close
  log.info(s"Embedded Kafka $kafkaBootstrap, data dir: $testDir")

  abstract override def afterAll(): Unit = {
    try {
      kafka.shutdown()
    } catch {
      case e: IllegalStateException => //
    } finally {
      def getRecursively(f: File): Seq[File] = f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles

      if (testDir.exists()) getRecursively(testDir).foreach(f => if (!f.delete()) throw new RuntimeException("Failed to delete " + f.getAbsolutePath))
    }
    super.afterAll()
  }

}
