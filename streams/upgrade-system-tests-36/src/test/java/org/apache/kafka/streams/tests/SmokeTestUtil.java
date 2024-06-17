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
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Instant;

public class SmokeTestUtil {

    final static int END = Integer.MAX_VALUE;

    static ProcessorSupplier<Object, Object, Void, Void> printProcessorSupplier(final String topic) {
        return printProcessorSupplier(topic, "");
    }

    static ProcessorSupplier<Object, Object, Void, Void> printProcessorSupplier(final String topic, final String name) {
        return () -> new ContextualProcessor<Object, Object, Void, Void>() {
            private int numRecordsProcessed = 0;
            private long smallestOffset = Long.MAX_VALUE;
            private long largestOffset = Long.MIN_VALUE;

            @Override
            public void init(final ProcessorContext<Void, Void> context) {
                super.init(context);
                System.out.println("[3.6] initializing processor: topic=" + topic + " taskId=" + context.taskId());
                System.out.flush();
                numRecordsProcessed = 0;
                smallestOffset = Long.MAX_VALUE;
                largestOffset = Long.MIN_VALUE;
            }

            @Override
            public void process(final Record<Object, Object> record) {
                numRecordsProcessed++;
                if (numRecordsProcessed % 100 == 0) {
                    System.out.printf("%s: %s%n", name, Instant.now());
                    System.out.println("processed " + numRecordsProcessed + " records from topic=" + topic);
                }

                context().recordMetadata().ifPresent(recordMetadata -> {
                    if (smallestOffset > recordMetadata.offset()) {
                        smallestOffset = recordMetadata.offset();
                    }
                    if (largestOffset < recordMetadata.offset()) {
                        largestOffset = recordMetadata.offset();
                    }
                });
            }

            @Override
            public void close() {
                System.out.printf("Close processor for task %s%n", context().taskId());
                System.out.println("processed " + numRecordsProcessed + " records");
                final long processed;
                if (largestOffset >= smallestOffset) {
                    processed = 1L + largestOffset - smallestOffset;
                } else {
                    processed = 0L;
                }
                System.out.println("offset " + smallestOffset + " to " + largestOffset + " -> processed " + processed);
                System.out.flush();
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
            return (key, value) -> new KeyValue<>(value == null ? null : Long.toString(value), 1L);
        }

        public Initializer<Long> init() {
            return () -> 0L;
        }

        Aggregator<String, Long, Long> adder() {
            return (aggKey, value, aggregate) -> aggregate + value;
        }

        Aggregator<String, Long, Long> remover() {
            return (aggKey, value, aggregate) -> aggregate - value;
        }
    }

    public static Serde<String> stringSerde = Serdes.String();

    public static Serde<Integer> intSerde = Serdes.Integer();

    static Serde<Long> longSerde = Serdes.Long();

    static Serde<Double> doubleSerde = Serdes.Double();

    public static void sleep(final long duration) {
        try {
            Thread.sleep(duration);
        } catch (final Exception ignore) { }
    }

}
