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

package io.amient.affinity.stream;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementations of StreamClient interface are used in CompactRDD and provide direct 1-1 mapping between
 * stream partitions to spark partitions.
 */
public interface StreamClient extends Serializable {

    List<Integer> getPartitions();

    Map<Long, Long> getOffsets(int partition);

    void publish(Iterator<PartitionedRecord<byte[], byte[]>> iter, Function<Long, Boolean> checker);


    /**
     * Underlying implementation may need to open resources specific for this iterator and must implement
     * the release(..) method to close them.
     *
     * @param partition
     * @param startOffset
     * @param stopOffset
     * @return
     */
    Iterator<Record<byte[], byte[]>> iterator(int partition, Long startOffset, Long stopOffset);

    void release(Iterator<Record<byte[], byte[]>> iter);

}