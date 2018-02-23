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

import java.net.MalformedURLException;
import java.net.URL;

public class CfgUrl extends Cfg<URL> {

    @Override
    public CfgUrl apply(Config config) {
        String urlString = listPos > -1 ? config.getStringList(relPath).get(listPos) : config.getString(relPath);
        try {
            return setValue(new URL(urlString));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
