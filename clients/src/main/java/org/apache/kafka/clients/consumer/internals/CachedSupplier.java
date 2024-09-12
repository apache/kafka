/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import java.util.function.Supplier;

/**
 * Simple {@link Supplier} that caches the initial creation of the object and stores it for later calls
 * to {@link #get()}.
 *
 * <p/>
 *
 * <em>Note</em>: this class is not thread safe! Use only in contexts which are designed/guaranteed to be
 * single-threaded.
 */
public abstract class CachedSupplier<T> implements Supplier<T> {

    private T result;

    protected abstract T create();

    @Override
    public T get() {
        if (result == null)
            result = create();

        return result;
    }
}
