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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class KTableSource<K, V> implements ProcessorSupplier<K, V> {

    public final String storeName;

    private boolean sendOldValues = false;

    public KTableSource(final String storeName) {
        this.storeName = storeName;
    }

    @Override
    public Processor<K, V> get() {
        return new KTableSourceProcessor();
    }

    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KTableSourceProcessor extends AbstractProcessor<K, V> {

        private KeyValueStore<K, V> store;
        private TupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<K, V>) context.getStateStore(storeName);
            tupleForwarder = new TupleForwarder<>(store, context, new ForwardingCacheFlushListener<K, V>(context, sendOldValues), sendOldValues);
        }

        @Override
        public void process(final K key, final V value) {
            // if the key is null, then ignore the record
            if (key == null) {
                return;
            }
            final V oldValue = store.get(key);
            store.put(key, value);
            tupleForwarder.maybeForward(key, value, oldValue);
        }
    }
}
