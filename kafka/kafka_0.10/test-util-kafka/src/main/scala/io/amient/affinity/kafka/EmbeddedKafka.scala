package io.amient.affinity.kafka

import java.io.File
import java.nio.file.Files
import java.util.Properties

import io.amient.affinity.avro.util.ZooKeeperClient
import io.amient.affinity.testutil.EmbeddedZooKeeper
import kafka.cluster.Broker
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.scalatest.{BeforeAndAfterAll, Suite}

trait EmbeddedKafka extends EmbeddedZooKeeper with BeforeAndAfterAll {

  self: Suite =>

  def numPartitions: Int

  private val testDir: File = Files.createTempDirectory(this.getClass.getSimpleName).toFile
  testDir.mkdirs()

  private val embeddedKafkaPath = new File(testDir, "local-kafka-logs")
  private val kafkaConfig = new KafkaConfig(new Properties {
    {
      put("broker.id", "1")
      put("host.name", "localhost")
      put("port", "0")
      put("log.dir", embeddedKafkaPath.toString)
      put("num.partitions", numPartitions.toString)
      put("auto.create.topics.enable", "true")
      put("zookeeper.connect", zkConnect)
    }
  })
  private val kafka = new KafkaServerStartable(kafkaConfig)
  kafka.startup()

  val tmpZkClient = new ZooKeeperClient(zkConnect)
  val broker = Broker.createBroker(1, tmpZkClient.readData[String]("/brokers/ids/1"))
  val kafkaBootstrap = broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT).connectionString()
  tmpZkClient.close
  println(s"Embedded Kafka $kafkaBootstrap, data dir: $testDir")

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
