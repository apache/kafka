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
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Collection;

public class KTablePassThrough<K, V> implements KTableProcessorSupplier<K, V, V> {
    private final Collection<KStreamAggProcessorSupplier> parents;

    KTablePassThrough(final Collection<KStreamAggProcessorSupplier> parents) {
        this.parents = parents;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTablePassThroughProcessor<>();
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        for (final KStreamAggProcessorSupplier parent : parents) {
            parent.enableSendingOldValues();
        }
        return true;
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {

        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KTablePassThroughValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[2];
            }
        };
    }

    private static final class KTablePassThroughProcessor<K, V> extends AbstractProcessor<K, V> {
        @Override
        public void process(final K key, final V value) {
            context().forward(key, value);
        }
    }

    private class KTablePassThroughValueGetter implements KTableValueGetter<K, V> {
        //private final KTableValueGetter<K, V> parentGetter;
        private final ValueAndTimestamp<V> timestamp = ValueAndTimestamp.make(null, 50L);

        KTablePassThroughValueGetter() {
        }

        @Override
        public void init(final ProcessorContext context) {
            //parentGetter.init(context);
        }

        @Override
        public ValueAndTimestamp<V> get(final K key) {
            //return parentGetter.get(key);
            return timestamp;
        }

        @Override
        public void close() {
            //parentGetter.close();
        }

    }
}
