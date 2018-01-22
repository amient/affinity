package io.amient.affinity.kafka

import java.nio.channels.ServerSocketChannel
import java.util.Properties

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

trait EmbeddedCfRegistry extends EmbeddedKafka with BeforeAndAfterAll {

  self: Suite =>

  private val log = LoggerFactory.getLogger(classOf[EmbeddedCfRegistry])

  private val registryConfig: SchemaRegistryConfig = new SchemaRegistryConfig(new Properties() {
    put("listeners", s"http://127.0.0.1:0")
    put("kafkastore.connection.url", zkConnect)
    put("avro.compatibility.level", "full")
    put("kafkastore.topic", "_schemas")
    put("debug", "true")
  })
  private val app = new SchemaRegistryRestApplication(registryConfig)
  private val registry = app.createServer
  registry.start()

  val registryUrl = s"http://127.0.0.1:" + registry.getConnectors.head.getTransport.asInstanceOf[ServerSocketChannel].socket.getLocalPort
  log.info("Confluent schema registry listening at: " + registryUrl)
  val registryClient = new CachedSchemaRegistryClient(registryUrl, 20)

  abstract override def afterAll() {
    registry.stop()
    super.afterAll()
  }

}
