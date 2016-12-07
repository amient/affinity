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

package io.amient.affinity.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface KafkaFetcher extends Closeable {

    public void close() throws IOException;

    Iterable<PayloadAndOffset> fetch(Long offset);
    //    {
//        val req = new FetchRequestBuilder().clientId(config.clientId).addFetch(topic, partition, offset, config.fetchMessageMaxBytes).build
//        val data = consumer.fetch(req).data(tap)
//        ErrorMapping.maybeThrowException(data.error)
//        data.messages
//    }

    Iterator<KeyPayloadAndOffset> iterator(Long startOffset, Long stopOffset);
//    new Iterator[(ByteKey, PayloadAndOffset)] {
//        private var messageAndOffset: MessageAndOffset = null
//        private var offset = startOffset
//        private var setIter = fetch(offset).iterator
//
//        seek
//
//        override def hasNext: Boolean = messageAndOffset != null
//
//        override def next(): (ByteKey, PayloadAndOffset) = {
//            if (messageAndOffset == null) {
//                throw new NoSuchElementException
//            } else {
//                val n = (new ByteKey(ByteUtils.bufToArray(messageAndOffset.message.key)),
//                new PayloadAndOffset(messageAndOffset.offset, ByteUtils.bufToArray(messageAndOffset.message.payload)))
//                seek
//                        n
//            }
//        }
//
//        private def seek {
//            while (offset < stopOffset) {
//                if (!setIter.hasNext) setIter = fetch(offset).iterator
//                messageAndOffset = setIter.next()
//                offset = messageAndOffset.nextOffset
//                if (messageAndOffset.message.payload != null && messageAndOffset.offset >= startOffset) {
//                    return
//                }
//            }
//            messageAndOffset = null
//        }
//
//    }

}
