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

import java.util.concurrent.Future

import io.amient.affinity.core.storage.{LogStorageConf, Record}
import io.amient.affinity.core.util.MappedJavaFuture
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

/**
  * This class is for simulating write failures in the KafkaStorage
  */
class FailingKafkaStorage(conf: LogStorageConf) extends KafkaLogStorage(conf) {

  override def append(record: Record[Array[Byte], Array[Byte]]): Future[java.lang.Long] = {
    val producerRecord = new ProducerRecord(topic, null, record.timestamp, record.key, record.value)
    new MappedJavaFuture[RecordMetadata, java.lang.Long](producer.send(producerRecord)) {
      override def map(result: RecordMetadata): java.lang.Long = {
        if (System.currentTimeMillis() % 3 == 0) throw new RuntimeException("simulated kafka producer error")
        result.offset()
      }
    }
  }

}
