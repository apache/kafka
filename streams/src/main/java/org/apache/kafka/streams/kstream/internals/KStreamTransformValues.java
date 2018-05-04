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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;

import java.io.File;
import java.util.Map;

public class KStreamTransformValues<K, V, R> implements ProcessorSupplier<K, V> {

    private final InternalValueTransformerWithKeySupplier<K, V, R> valueTransformerSupplier;

    public KStreamTransformValues(final InternalValueTransformerWithKeySupplier<K, V, R> valueTransformerSupplier) {
        this.valueTransformerSupplier = valueTransformerSupplier;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamTransformValuesProcessor<>(valueTransformerSupplier.get());
    }

    public static class KStreamTransformValuesProcessor<K, V, R> implements Processor<K, V> {

        private final InternalValueTransformerWithKey<K, V, R> valueTransformer;
        private ProcessorContext context;

        public KStreamTransformValuesProcessor(final InternalValueTransformerWithKey<K, V, R> valueTransformer) {
            this.valueTransformer = valueTransformer;
        }

        @Override
        public void init(final ProcessorContext context) {
            valueTransformer.init(
                new ProcessorContext() {
                    @Override
                    public String applicationId() {
                        return context.applicationId();
                    }

                    @Override
                    public TaskId taskId() {
                        return context.taskId();
                    }

                    @Override
                    public Serde<?> keySerde() {
                        return context.keySerde();
                    }

                    @Override
                    public Serde<?> valueSerde() {
                        return context.valueSerde();
                    }

                    @Override
                    public File stateDir() {
                        return context.stateDir();
                    }

                    @Override
                    public StreamsMetrics metrics() {
                        return context.metrics();
                    }

                    @Override
                    public void register(final StateStore store,
                                         final StateRestoreCallback stateRestoreCallback) {
                        context.register(store, stateRestoreCallback);
                    }

                    @Override
                    public StateStore getStateStore(final String name) {
                        return context.getStateStore(name);
                    }

                    @Override
                    public Cancellable schedule(final long interval, final PunctuationType type, final Punctuator callback) {
                        return context.schedule(interval, type, callback);
                    }

                    @Override
                    public <K, V> void forward(final K key, final V value) {
                        throw new StreamsException("ProcessorContext#forward() must not be called within TransformValues.");
                    }

                    @Override
                    public <K, V> void forward(final K key, final V value, final To to) {
                        throw new StreamsException("ProcessorContext#forward() must not be called within TransformValues.");
                    }

                    @SuppressWarnings("deprecation")
                    @Override
                    public <K, V> void forward(final K key, final V value, final int childIndex) {
                        throw new StreamsException("ProcessorContext#forward() must not be called within TransformValues.");
                    }

                    @SuppressWarnings("deprecation")
                    @Override
                    public <K, V> void forward(final K key, final V value, final String childName) {
                        throw new StreamsException("ProcessorContext#forward() must not be called within TransformValues.");
                    }

                    @Override
                    public void commit() {
                        context.commit();
                    }

                    @Override
                    public String topic() {
                        return context.topic();
                    }

                    @Override
                    public int partition() {
                        return context.partition();
                    }

                    @Override
                    public long offset() {
                        return context.offset();
                    }

                    @Override
                    public long timestamp() {
                        return context.timestamp();
                    }

                    @Override
                    public Map<String, Object> appConfigs() {
                        return context.appConfigs();
                    }

                    @Override
                    public Map<String, Object> appConfigsWithPrefix(String prefix) {
                        return context.appConfigsWithPrefix(prefix);
                    }
                });
            this.context = context;
        }

        @Override
        public void process(K key, V value) {
            context.forward(key, valueTransformer.transform(key, value));
        }

        @Override
        public void close() {
            valueTransformer.close();
        }
    }
}
