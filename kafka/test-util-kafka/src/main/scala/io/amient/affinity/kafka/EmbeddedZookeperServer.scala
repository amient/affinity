package io.amient.affinity.kafka

import java.io.File
import java.net.InetSocketAddress

import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.slf4j.LoggerFactory

trait EmbeddedZookeperServer extends EmbeddedService {

  private val logger = LoggerFactory.getLogger(classOf[EmbeddedZookeperServer])

  private val embeddedZkPath = new File(testDir, "local-zookeeper")
  // smaller testDir footprint, default zookeeper file blocks are 65535Kb
  System.getProperties().setProperty("zookeeper.preAllocSize", "64")
  private val zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000)
  private val zkFactory = new NIOServerCnxnFactory
  zkFactory.configure(new InetSocketAddress(0), 10)
  val zkConnect = "localhost:" + zkFactory.getLocalPort
  logger.info(s"Embedded ZooKeeper $zkConnect, data directory: $testDir")
  zkFactory.startup(zookeeper)

  abstract override def close(): Unit = try zkFactory.shutdown() finally super.close

}
