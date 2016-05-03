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

package org.apache.kafka.streams.smoketest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.io.File;

public class SmokeTestUtil {

    public final static int WINDOW_SIZE = 100;
    public final static long START_TIME = 60000L * 60 * 24 * 365 * 30;
    public final static int END = Integer.MAX_VALUE;

    public static <T> ProcessorSupplier<String, T> printProcessorSupplier(final String topic) {
        return printProcessorSupplier(topic, false);
    }

    public static <T> ProcessorSupplier<String, T> printProcessorSupplier(final String topic, final boolean printOffset) {
        return new ProcessorSupplier<String, T>() {
            public Processor<String, T> get() {
                return new Processor<String, T>() {
                    private int numRecordsProcessed = 0;
                    private ProcessorContext context;

                    @Override
                    public void init(ProcessorContext context) {
                        System.out.println("initializing processor: topic=" + topic + " taskId=" + context.taskId());
                        numRecordsProcessed = 0;
                        this.context = context;
                    }

                    @Override
                    public void process(String key, T value) {
                        if (printOffset) System.out.println(">>> " + context.offset());
                        numRecordsProcessed++;
                        if (numRecordsProcessed % 100 == 0) {
                            System.out.println("processed " + numRecordsProcessed + " records from topic=" + topic);
                        }
                    }

                    @Override
                    public void punctuate(long timestamp) {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
    }

    public static final class Unwindow<K, V> implements KeyValueMapper<Windowed<K>, V, KeyValue<K, V>> {
        public KeyValue<K, V> apply(Windowed<K> winKey, V value) {
            return new KeyValue<K, V>(winKey.key(), value);
        }
    }

    public static class Agg {

        public KeyValueMapper<String, Long, KeyValue<String, Long>> selector() {
            return new KeyValueMapper<String, Long, KeyValue<String, Long>>() {
                @Override
                public KeyValue<String, Long> apply(String key, Long value) {
                    return new KeyValue<>(Long.toString(value), 1L);
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

        public Aggregator<String, Long, Long> adder() {
            return new Aggregator<String, Long, Long>() {
                @Override
                public Long apply(String aggKey, Long value, Long aggregate) {
                    return aggregate + value;
                }
            };
        }

        public Aggregator<String, Long, Long> remover() {
            return new Aggregator<String, Long, Long>() {
                @Override
                public Long apply(String aggKey, Long value, Long aggregate) {
                    return aggregate - value;
                }
            };
        }
    }

    public static Serde<String> stringSerde = Serdes.String();

    public static Serde<Integer> intSerde = Serdes.Integer();

    public static Serde<Long> longSerde = Serdes.Long();

    public static Serde<Double> doubleSerde = Serdes.Double();

    public static File createDir(String path) throws Exception {
        File dir = new File(path);

        dir.mkdir();

        return dir;
    }

    public static File createDir(File parent, String child) throws Exception {
        File dir = new File(parent, child);

        dir.mkdir();

        return dir;
    }

    public static void sleep(long duration) {
        try {
            Thread.sleep(duration);
        } catch (Exception ex) {
            //
        }
    }


}
