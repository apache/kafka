/**
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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CachedStateStore;

public class KTableSource<K, V> implements ProcessorSupplier<K, V> {

    public final String storeName;

    private boolean materialized = false;
    private boolean sendOldValues = false;

    public KTableSource(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public Processor<K, V> get() {
        return materialized ? new MaterializedKTableSourceProcessor() : new KTableSourceProcessor();
    }

    public void materialize() {
        materialized = true;
    }

    public boolean isMaterialized() {
        return materialized;
    }

    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KTableSourceProcessor extends AbstractProcessor<K, V> {
        @Override
        public void process(K key, V value) {
            // the keys should never be null
            if (key == null)
                throw new StreamsException("Record key for the source KTable from store name " + storeName + " should not be null.");

            context().forward(key, new Change<>(value, null));
        }
    }

    private class MaterializedKTableSourceProcessor extends AbstractProcessor<K, V> {

        private KeyValueStore<K, V> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<K, V>) context.getStateStore(storeName);
            ((CachedStateStore) store).setFlushListener(new ForwardingCacheFlushListener<K, V>(context, sendOldValues));
        }

        @Override
        public void process(K key, V value) {
            // the keys should never be null
            if (key == null)
                throw new StreamsException("Record key for the source KTable from store name " + storeName + " should not be null.");

            store.put(key, value);
        }
    }
}
