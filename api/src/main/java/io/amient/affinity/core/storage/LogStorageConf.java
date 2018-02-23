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

import io.amient.affinity.core.config.CfgCls;
import io.amient.affinity.core.config.CfgLong;
import io.amient.affinity.core.config.CfgStruct;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class LogStorageConf extends CfgStruct<LogStorageConf> {
    public CfgCls<LogStorage> Class = cls("class", LogStorage.class, false);
    public CfgLong MinTimestamp = longint("min.timestamp.ms", 0L);

    @Override
    protected Set<String> specializations() {
        return new HashSet<>(Arrays.asList("kafka"));
    }

}
