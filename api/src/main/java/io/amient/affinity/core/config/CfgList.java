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

package io.amient.affinity.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class CfgList<L, C extends Cfg<L>> extends Cfg<List<L>> {

    private final Class<C> cls;

    public CfgList(Class<C> cls) {
        this.cls = cls;
    }

    @Override
    public Cfg<List<L>> apply(Config config) throws IllegalArgumentException {
        List<L> list = new LinkedList<>();
        Iterator<ConfigValue> it = config.getList(relPath).iterator();
        int i = 0;
        while(it.hasNext()) {
            try {
                ConfigValue el = it.next();
                C item = cls.newInstance();
                item.setListPos(i);
                item.setRelPath(relPath);
                item.apply(config);
                list.add((L)el.unwrapped());
            } catch (InstantiationException e) {
                throw new IllegalArgumentException(e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(e);
            }

        }
        setValue(list);
        return this;
    }

    @Override
    public String parameterInfo() {
        return "[]";
    }

    public L apply(Integer entry) {
        return apply().get(entry);
    }

}
