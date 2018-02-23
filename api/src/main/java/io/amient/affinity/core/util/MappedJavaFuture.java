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

package io.amient.affinity.core.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

abstract public class MappedJavaFuture<A, T> implements Future<T> {

    final private Future<A> f;

    public MappedJavaFuture(Future<A> f) {
        this.f = f;
    }

    @Override
    public boolean isCancelled() {
        return f.isCancelled();
    }

    public abstract T map(A result);

    public T recover(Throwable e) throws Throwable {
        throw e;
    }

    public T get() throws ExecutionException, InterruptedException {
        try {
            return map(f.get());
        } catch (Throwable e) {
            try {
                return recover(e);
            } catch (Throwable e1) {
                throw new RuntimeException(e1);
            }
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return map(f.get(timeout, unit));
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return f.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isDone() {
        return f.isDone();
    }
}
