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

package io.amient.affinity.core.storage.rocksdb

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}

import com.typesafe.config.Config
import io.amient.affinity.core.storage.MemStore
import io.amient.affinity.core.util.ByteUtils
import org.rocksdb.{Options, RocksDB, RocksDBException, RocksIterator}


object MemStoreRocksDb {
  val CONFIG_ROCKSDB_DATA_PATH = "memstore.rocksdb.data.path"

  private val refs = scala.collection.mutable.Map[String, Long]()
  private val instances = scala.collection.mutable.Map[String, RocksDB]()

  RocksDB.loadLibrary()

  def createOrGetRocksDbInstanceRef(pathToData: String, rocksOptions: Options): RocksDB = synchronized {
    if (refs.contains(pathToData) && refs(pathToData) > 0) {
      refs.put(pathToData, refs(pathToData) + 1)
      return instances(pathToData)
    } else {
      val instance = try {
        RocksDB.open(rocksOptions, pathToData)
      } catch {
        case e:RocksDBException => throw new RuntimeException(e)
      }
      instances.put(pathToData, instance)
      refs.put(pathToData, 1)
      instance
    }
  }

  def releaseRocksDbInstance(pathToData: String): Unit = synchronized {
    if (refs(pathToData) > 1) {
      refs.put(pathToData, refs(pathToData) - 1)
    } else {
      refs.remove(pathToData)
      instances(pathToData).close()
    }
  }

}

class MemStoreRocksDb(config: Config, partition: Int) extends MemStore {

  import MemStoreRocksDb._

  private val pathToData = config.getString(CONFIG_ROCKSDB_DATA_PATH) + s"/$partition"
  private val containerPath = Paths.get(pathToData).getParent.toAbsolutePath
  private val rocksOptions = new Options().setCreateIfMissing(true)

  Files.createDirectories(containerPath)

  private val internal: RocksDB = createOrGetRocksDbInstanceRef(pathToData, rocksOptions)

  override def close: Unit = releaseRocksDbInstance(pathToData)

  override def apply(key: MK): Option[MV] = get(ByteUtils.bufToArray(key))

  override def iterator:Iterator[(MK,MV)] = new Iterator[(MK, MV)] {
    private var rocksIterator: RocksIterator = null
    override def hasNext: Boolean = {
      if (rocksIterator == null) {
        rocksIterator = internal.newIterator()
        rocksIterator.seekToFirst()
      } else {
        rocksIterator.next()
      }
      if (!rocksIterator.isValid) rocksIterator.close
      rocksIterator.isValid
    }

    override def next(): (MK, MV) = (ByteBuffer.wrap(rocksIterator.key()), ByteBuffer.wrap(rocksIterator.value()))

  }

  override protected[storage] def update(key: MK, value: MV): Option[MV] = {
    val keyBytes = ByteUtils.bufToArray(key)
    val prev = get(keyBytes)
    internal.put(keyBytes, ByteUtils.bufToArray(value))
    prev
  }

  override protected[storage] def remove(key: MK):Option[MV] = {
    val keyBytes = ByteUtils.bufToArray(key)
    val prev = get(keyBytes)
    internal.remove(keyBytes)
    prev
  }

  private def get(key: Array[Byte]): Option[MV] = internal.get(key) match {
    case null => None
    case value => Some(ByteBuffer.wrap(value))
  }

}
