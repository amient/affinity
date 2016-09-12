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

package io.amient.affinity.core.actor

import java.util.concurrent.CopyOnWriteArrayList

import akka.event.Logging
import io.amient.affinity.core.storage.Storage
import scala.collection.JavaConverters._

trait Partition extends Service {

  override val log = Logging.getLogger(context.system, this)

  /**
    * physical partition id - this is read from the name of the Partition Actor;  assigned by the Region
    */
  val partition = self.path.name.toInt

  private val storageRegistry = new CopyOnWriteArrayList[Storage[_, _]]()

  def storage[K, V](creator: => Storage[K, V]): Storage[K, V] = {
    val storage: Storage[K, V] = creator
    storage.boot()
    storageRegistry.add(storage)
    storage
  }

  override protected def onBecomeMaster: Unit = {
    storageRegistry.asScala.foreach { storage =>
      storage.untail()
    }
  }

  override protected def onBecomeStandby: Unit = {
    storageRegistry.asScala.foreach { storage =>
      storage.tail()
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    storageRegistry.asScala.foreach { storage =>
      storage.close()
    }
  }

}
