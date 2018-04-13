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
package org.apache.kafka.streams.tests;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.io.File;

public class SmokeTestUtil {

    final static int END = Integer.MAX_VALUE;

    static ProcessorSupplier<Object, Object> printProcessorSupplier(final String topic) {
        return printProcessorSupplier(topic, false);
    }

    private static ProcessorSupplier<Object, Object> printProcessorSupplier(final String topic, final boolean printOffset) {
        return new ProcessorSupplier<Object, Object>() {
            @Override
            public Processor<Object, Object> get() {
                return new AbstractProcessor<Object, Object>() {
                    private int numRecordsProcessed = 0;

                    @Override
                    public void init(final ProcessorContext context) {
                        System.out.println("initializing processor: topic=" + topic + " taskId=" + context.taskId());
                        numRecordsProcessed = 0;
                    }

                    @Override
                    public void process(final Object key, final Object value) {
                        numRecordsProcessed++;
                        if (numRecordsProcessed % 100 == 0) {
                            System.out.println(System.currentTimeMillis());
                            System.out.println("processed " + numRecordsProcessed + " records from topic=" + topic);
                        }
                    }

                    @Override
                    public void punctuate(final long timestamp) {}

                    @Override
                    public void close() {}
                };
            }
        };
    }

    public static final class Unwindow<K, V> implements KeyValueMapper<Windowed<K>, V, K> {
        @Override
        public K apply(final Windowed<K> winKey, final V value) {
            return winKey.key();
        }
    }

    public static class Agg {

        KeyValueMapper<String, Long, KeyValue<String, Long>> selector() {
            return new KeyValueMapper<String, Long, KeyValue<String, Long>>() {
                @Override
                public KeyValue<String, Long> apply(final String key, final Long value) {
                    return new KeyValue<>(value == null ? null : Long.toString(value), 1L);
                }
            };
        }

        public Initializer<Long> init() {
            return new Initializer<Long>() {
                @Override
                public Long apply() {
                    return 0L;
                }
            };
        }

        Aggregator<String, Long, Long> adder() {
            return new Aggregator<String, Long, Long>() {
                @Override
                public Long apply(final String aggKey, final Long value, final Long aggregate) {
                    return aggregate + value;
                }
            };
        }

        Aggregator<String, Long, Long> remover() {
            return new Aggregator<String, Long, Long>() {
                @Override
                public Long apply(final String aggKey, final Long value, final Long aggregate) {
                    return aggregate - value;
                }
            };
        }
    }

    public static Serde<String> stringSerde = Serdes.String();

    public static Serde<Integer> intSerde = Serdes.Integer();

    static Serde<Long> longSerde = Serdes.Long();

    static Serde<Double> doubleSerde = Serdes.Double();

    static File createDir(final File parent, final String child) {
        final File dir = new File(parent, child);

        dir.mkdir();

        return dir;
    }

    public static void sleep(final long duration) {
        try {
            Thread.sleep(duration);
        } catch (final Exception ignore) { }
    }

}
