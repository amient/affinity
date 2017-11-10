package io.amient.affinity.testutil

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files

import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait EmbeddedZooKeeper extends BeforeAndAfterAll {

  self: Suite =>

  private val testDir: File = Files.createTempDirectory(this.getClass.getSimpleName).toFile
  println(s"Embedded ZooKeeper test dir: $testDir")
  testDir.mkdirs()

  private val embeddedZkPath = new File(testDir, "local-zookeeper")
  // smaller testDir footprint, default zookeeper file blocks are 65535Kb
  System.getProperties().setProperty("zookeeper.preAllocSize", "64")
  private val zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000)
  private val zkFactory = new NIOServerCnxnFactory
  zkFactory.configure(new InetSocketAddress(0), 10)
  val zkConnect = "localhost:" + zkFactory.getLocalPort
  zkFactory.startup(zookeeper)

  abstract override def afterAll(): Unit = {
    try {
      zkFactory.shutdown()
    } finally {
      def getRecursively(f: File): Seq[File] = f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles
      if (testDir.exists()) getRecursively(testDir).foreach(f => if (!f.delete()) throw new RuntimeException("Failed to delete " + f.getAbsolutePath))
    }
    super.afterAll()
  }
}
