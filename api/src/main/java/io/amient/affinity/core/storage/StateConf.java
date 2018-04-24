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

package io.amient.affinity.core.storage;

import io.amient.affinity.core.config.*;
import io.amient.affinity.core.storage.MemStore.MemStoreConf;

public class StateConf extends CfgStruct<StateConf> {

    public Cfg<Integer> TtlSeconds = integer("ttl.sec", -1)
            .doc("Per-record expiration which will based off event-time if the data class implements EventTime trait");

    public Cfg<Boolean> External = bool("external", true, false)
            .doc("If the state is attached to a data stream which is populated and partitioned by an external process - external state becomes readonly");

    public Cfg<Long> MinTimestampUnixMs = longint("min.timestamp.ms", 0L)
            .doc("Any records with timestamp lower than this value will be immediately dropped");

    public LogStorageConf Storage = struct("storage", new LogStorageConf());

    public MemStoreConf MemStore = struct("memstore", new MemStore.MemStoreConf());

    public Cfg<Integer> LockTimeoutMs = integer("lock.timeout.ms", 10000)
            .doc("When per-row locking is used, this time-out specifies how long a lock can be held by a single thread");
}

