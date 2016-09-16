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

import akka.event.Logging

trait Partition extends Service with ActorState {

  override val log = Logging.getLogger(context.system, this)

  /**
    * physical partition id - this is read from the name of the Partition Actor;  assigned by the Region
    */
  val partition = self.path.name.toInt

  /**
    * onBecomeMaster is signalling that the partition should take over the responsibility
    * of being the Master for the related physical partition. The signalling message
    * may be resent as part of ack contract so this method must be idempotent.
    */
  override protected def onBecomeMaster: Unit = {
    log.info(s"Became leader for partition $partition")
    untailState()
  }

  /**
    * onBecomeStandby is signalling that the partition should become a passive standby
    * and keep listening to the changes in the related physical partition.
    * The signalling message may be resent as part of ack contract so this method must be idempotent.
    */
  override protected def onBecomeStandby: Unit = {
    log.info(s"Became standby for partition $partition")
    tailState()
  }

  override def postStop(): Unit = {
    super.postStop()
    log.info(s"Closing all state in partition $partition")
    closeState()
  }

}
