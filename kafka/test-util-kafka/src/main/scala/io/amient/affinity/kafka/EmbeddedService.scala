package io.amient.affinity.kafka

import java.io.{Closeable, File}
import java.nio.file.Files

private[kafka] trait EmbeddedService extends Closeable {

  val testDir: File = Files.createTempDirectory(this.getClass.getSimpleName).toFile
  testDir.mkdirs()

  override def close(): Unit = {
    def deleteDirectory(f: File): Unit = if (f.exists) {
      if (f.isDirectory) f.listFiles.foreach(deleteDirectory)
      if (!f.delete) throw new RuntimeException(s"Failed to delete ${f.getAbsolutePath}")
    }
    deleteDirectory(testDir)
  }

}
