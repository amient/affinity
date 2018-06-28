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

public class CfgList<L> extends Cfg<List<L>> {

    public final Class<? extends Cfg<L>> cls;

    public CfgList(Class<? extends Cfg<L>> cls) {
        this.cls = cls;
    }

    @Override
    public Cfg<List<L>> apply(Config config) throws IllegalArgumentException {
        List<L> list = new LinkedList<>();
        Iterator<ConfigValue> it = config.getList(relPath).iterator();
        int i = 0;
        while (it.hasNext()) {
            try {
                ConfigValue el = it.next();
                Cfg<L> item = cls.newInstance();
                item.setListPos(i);
                item.setRelPath(relPath);
                switch (el.valueType()) {
                    case OBJECT:
                        list.add((L) item.apply(el.atKey("x").getConfig("x")));
                        break;
                    default:
                        list.add((L) el.unwrapped());
                        break;
                }
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
        return "";
    }

    public L apply(Integer entry) {
        List<L> list = apply();
        return list.get(entry);
    }

    @Override
    public CfgList<L> doc(String description) {
        super.doc(description);
        return this;
    }

}
