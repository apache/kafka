/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.state.KeyValueStore;

/**
 * A {@link KeyValueStore} storage engine wrapper for utilities like logging, caching, and metering.
 */
interface WrapperKeyValueStore<K, V> extends WrappedStateStore, KeyValueStore<K, V> {

    abstract class AbstractKeyValueStore<K, V> extends WrappedStateStore.AbstractStateStore implements WrapperKeyValueStore<K, V> {
        private final KeyValueStore<?, ?> inner;

        AbstractKeyValueStore(KeyValueStore<?, ?> inner) {
            super(inner);

            this.inner = inner;
        }

        @Override
        public long approximateNumEntries() {
            return this.inner.approximateNumEntries();
        }
    }
}
