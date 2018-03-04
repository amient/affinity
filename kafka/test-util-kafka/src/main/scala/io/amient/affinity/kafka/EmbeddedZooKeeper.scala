/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.kafka

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files

import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

trait EmbeddedZooKeeper extends EmbeddedService {

  self: Suite =>

  private val log = LoggerFactory.getLogger(classOf[EmbeddedZooKeeper])

  private val embeddedZkPath = new File(testDir, "local-zookeeper")
  // smaller testDir footprint, default zookeeper file blocks are 65535Kb
  System.getProperties().setProperty("zookeeper.preAllocSize", "64")
  private val zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000)
  private val zkFactory = new NIOServerCnxnFactory
  zkFactory.configure(new InetSocketAddress(0), 10)
  val zkConnect = "localhost:" + zkFactory.getLocalPort
  log.info(s"Embedded ZooKeeper $zkConnect, data directory: $testDir")
  zkFactory.startup(zookeeper)

  abstract override def afterAll(): Unit = try {
    zkFactory.shutdown()
  } finally {
    super.afterAll()
  }

}
