package io.amient.affinity.kafka

import java.io.File
import java.nio.file.Files

import org.scalatest.{BeforeAndAfterAll, Suite}

private[kafka] trait EmbeddedService extends BeforeAndAfterAll {
  self: Suite =>

  protected val testDir: File = Files.createTempDirectory(this.getClass.getSimpleName).toFile
  testDir.mkdirs()

  private def deleteDirectory(f: File): Unit = if (f.exists) {
    if (f.isDirectory) f.listFiles.foreach(deleteDirectory)
    if (!f.delete) throw new RuntimeException(s"Failed to delete ${f.getAbsolutePath}")
  }

  abstract override def afterAll(): Unit = {
    try {
      deleteDirectory(testDir)
    } finally {
      super.afterAll()
    }
  }

}
