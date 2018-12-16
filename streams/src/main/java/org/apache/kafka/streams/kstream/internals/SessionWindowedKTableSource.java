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

import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

public class SessionWindowedKTableSource<K, V> extends KTableSource<Windowed<K>, V> {

    public SessionWindowedKTableSource(final String storeName, final String queryableName) {
        super(storeName, queryableName);
    }

    @Override
    public Processor<Windowed<K>, V> get() {
        return new SessionKTableSourceProcessor<>();
    }

    private class SessionKTableSourceProcessor<K, V> extends AbstractProcessor<Windowed<K>, V> {

        private SessionStore<K, V> sessionStore;
        private TupleForwarder<Windowed<K>, V> tupleForwarder;
        private StreamsMetricsImpl metrics;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            sessionStore = (SessionStore<K, V>) context.getStateStore(storeName);
            metrics = (StreamsMetricsImpl) context.metrics();
            tupleForwarder = new TupleForwarder<>(sessionStore, context, new ForwardingCacheFlushListener<K, V>(context), sendOldValues);
        }

        @Override
        public void process(Windowed<K> key, V value) {
            // if the key is null, then ignore the record
            if (key == null) {
                LOG.warn(
                    "Skipping record due to null key. topic=[{}] partition=[{}] offset=[{}]",
                    context().topic(), context().partition(), context().offset()
                );
                metrics.skippedRecordsSensor().record();
                return;
            }

            try (final KeyValueIterator<Windowed<K>, V> iter = sessionStore.findSessions(key.key(), key.window().start(), key.window().end())) {
                V oldValue = null;
                if (iter.hasNext()) {
                    oldValue = iter.next().value;
                    if (iter.hasNext()) {
                        throw new ProcessorStateException(String.format("Iterator for key [%s] on session store has more than one value", key));
                    }
                }

                sessionStore.put(key, value);
                tupleForwarder.maybeForward(key, value, oldValue);
            }
        }
    }
}
