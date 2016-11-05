/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.testutil

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files

import com.typesafe.config.{Config, ConfigValueFactory}
import io.amient.affinity.core.cluster.CoordinatorZk
import io.amient.affinity.core.serde.avro.schema.ZkAvroSchemaRegistry
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

trait SystemTestBaseWithZk extends SystemTestBase {

  val testDir: File = Files.createTempDirectory(this.getClass.getSimpleName).toFile
  println(s"Test dir: $testDir")
  testDir.mkdirs()

  private val embeddedZkPath = new File(testDir, "local-zookeeper")
  // smaller testDir footprint, default zookeeper file blocks are 65535Kb
  System.getProperties().setProperty("zookeeper.preAllocSize", "64")
  private val zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000)
  private val zkFactory = new NIOServerCnxnFactory
  zkFactory.configure(new InetSocketAddress(0), 10)
  val zkConnect = "localhost:" + zkFactory.getLocalPort
  zkFactory.startup(zookeeper)

  override def configure(config: Config): Config = super.configure(config)
    .withValue(CoordinatorZk.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect))
    .withValue(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect))

  override def afterAll(): Unit = {
    try {
      zkFactory.shutdown()
    } finally {
      deleteDirectory(testDir)
    }
  }


}
