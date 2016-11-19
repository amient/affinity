///*
// * Copyright 2016 Michal Harish, michal.harish@gmail.com
// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.amient.affinity.core.storage.rocksdb
//
//import akka.actor.Status
//import akka.pattern.ask
//import akka.util.Timeout
//import io.amient.affinity.core.actor.Partition
//import io.amient.affinity.core.cluster.Node
//import io.amient.affinity.core.serde.primitive.StringSerde
//import io.amient.affinity.testutil.SystemTestBase
//import org.rocksdb.{Options, RocksDB}
//import org.scalatest.{FlatSpec, Matchers}
//
//import scala.concurrent.{Await, Future}
//import scala.concurrent.duration._
//import scala.language.postfixOps
//import scala.util.{Failure, Success}
//
//class MemStoreRocksDbSpec extends FlatSpec with Matchers with SystemTestBase {
//
//  val config = configure("rocksdb-test")
//
//  val pathToData = config.getString("affinity.state.test.memstore.rocksdb.data.path")
//
//  "MemStoreRocksDb" should "store all changes in the underlying rocksdb store" in {
//
//    val node = new Node(config) {
//
//      Await.ready(startRegion(new Partition {
//        val kvstore = state[String, String]("test")
//
//        import context.dispatcher
//
//        override def handle: Receive = {
//          case "ping" => sender ! "pong"
//          case key: String => sender ! kvstore(key)
//          case cmd @ (key: String, value) =>
//            val origin = sender
//            val result = value match {
//              case None => kvstore.remove(key, cmd)
//              case Some(value: String) =>
//                val f = kvstore.update(key, value)
//                if (kvstore.iterator.exists(such => such._1 == key && such._2 == value)) f else
//                  Future.failed(new NoSuchElementException)
//
//            }
//            result onComplete {
//              case Success(prev) => origin ! prev
//              case Failure(e) => origin ! Status.Failure(e)
//            }
//        }
//      }), 5 seconds)
//    }
//
//    try {
//      val x = node.system.actorSelection("/user/controller/region/0")
//      implicit val timeout = Timeout(1 second)
//      Await.result(x ? "ping", timeout.duration) should be("pong")
//      Await.result(x ? ("hello", None), timeout.duration)
//      Await.result(x ? "hello", timeout.duration) should be(None)
//      Await.result(x ? ("hello", Some("world")), timeout.duration) should be(None)
//      Await.result(x ? "hello", timeout.duration) should be(Some("world"))
//      Await.result(x ? ("hello", Some("world2")), timeout.duration) should be(Some("world"))
//    } finally {
//      node.shutdown()
//    }
//
//    val db = RocksDB.open(new Options(), s"$pathToData/0")
//    try {
//      val serde = new StringSerde
//      db.get(serde.toBytes("hello")) should be(serde.toBytes("world2"))
//    } finally {
//      db.close()
//    }
//
//  }
//
//
//}
