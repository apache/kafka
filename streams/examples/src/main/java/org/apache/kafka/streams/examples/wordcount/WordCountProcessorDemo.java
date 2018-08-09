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
package org.apache.kafka.streams.examples.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueWithTimestampStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates, using the low-level Processor APIs, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 * <p>
 * <strong>Note: This is simplified code that only works correctly for single partition input topics.
 * Check out {@link WordCountDemo} for a generic example.</strong>
 * <p>
 * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-processor-output" where each record
 * is an updated count of a single word.
 * <p>
 * Before running this example you must create the input topic and the output topic (e.g. via
 * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
 * {@code bin/kafka-console-producer.sh}). Otherwise you won't see any data arriving in the output topic.
 */
public final class WordCountProcessorDemo {

    static class MyProcessorSupplier implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(1000, PunctuationType.STREAM_TIME, timestamp -> {
                        try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
                            System.out.println("----------- " + timestamp + " ----------- ");

                            while (iter.hasNext()) {
                                final KeyValue<String, Integer> entry = iter.next();

                                System.out.println("[" + entry.key + ", " + entry.value + "]");

                                context.forward(entry.key, entry.value.toString());
                            }
                        }
                    });
                    this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("Counts");
                }

                @Override
                public void process(final String dummy, final String line) {
                    final String[] words = line.toLowerCase(Locale.getDefault()).split(" ");

                    for (final String word : words) {
                        final Integer oldValue = this.kvStore.get(word);

                        if (oldValue == null) {
                            this.kvStore.put(word, 1);
                        } else {
                            this.kvStore.put(word, oldValue + 1);
                        }
                    }

                    context.commit();
                }

                @Override
                public void close() {}
            };
        }
    }

    static class MyProcessorSupplierNew implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;
                private KeyValueWithTimestampStore<String, Integer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(1000, PunctuationType.STREAM_TIME, timestamp -> {
                        try (final KeyValueIterator<String, ValueAndTimestamp<Integer>> iter = kvStore.all()) {
                            System.out.println("----------- " + timestamp + " ----------- ");

                            while (iter.hasNext()) {
                                final KeyValue<String, ValueAndTimestamp<Integer>> entry = iter.next();

                                System.out.println("[" + entry.key + ", " + entry.value.value() + "]:" + entry.value.timestamp());

                                context.forward(entry.key,
                                                entry.value.value().toString(),
                                                To.all().withTimestamp(entry.value.timestamp()));
                            }
                        }
                    });
                    this.kvStore = (KeyValueWithTimestampStore<String, Integer>) context.getStateStore("Counts");
                }

                @Override
                public void process(final String dummy, final String line) {
                    final String[] words = line.toLowerCase(Locale.getDefault()).split(" ");

                    for (final String word : words) {
                        final ValueAndTimestamp<Integer> oldValue = this.kvStore.get(word);

                        if (oldValue.value() == null) {
                            this.kvStore.put(word, 1, context.timestamp());
                        } else {
                            this.kvStore.put(word,
                                             oldValue.value() + 1,
                                             Math.max(oldValue.timestamp(), context.timestamp()));
                        }
                    }

                    context.commit();
                }

                @Override
                public void close() {}
            };
        }
    }

    static KafkaStreams getStreamsClient(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Topology builder = new Topology();

        builder.addSource("Source", "streams-plaintext-input");

        if (args.length > 0 && args[0].equals("--run-with-old-store")) {
            builder.addProcessor("Process", new MyProcessorSupplier(), "Source");
            builder.addStateStore(
                Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore("Counts"),
                    Serdes.String(),
                    Serdes.Integer()),
                "Process");
        } else if (args.length > 0 && args[0].equals("--run-with-upgrade-store")) {
            if (args.length > 1) {
                props.put(StreamsConfig.UPGRADE_MODE_CONFIG, args[1]);
            }
            builder.addProcessor("Process", new MyProcessorSupplierNew(), "Source");
            builder.addStateStore(
                Stores.keyValueToKeyValueWithTimestampUpgradeStoreBuilder(
                    Stores.persistentKeyValueStore("Counts"),
                    Serdes.String(),
                    Serdes.Integer()),
                "Process");
        } else if (args.length > 0 && args[0].equals("--run-with-new-store")) {
            builder.addProcessor("Process", new MyProcessorSupplierNew(), "Source");
            builder.addStateStore(
                Stores.keyValueWithTimestampStoreBuilder(
                    Stores.persistentKeyValueStore("Counts"),
                    Serdes.String(),
                    Serdes.Integer()),
                "Process");
        } else {
            throw new RuntimeException("Required argument missing");
        }

        builder.addSink("Sink", "streams-wordcount-processor-output", "Process");

        return new KafkaStreams(builder, props);
    }

    public static void main(final String[] args) {
        final KafkaStreams streams = getStreamsClient(args);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        if (args[0].equals("--run-with-old-store")) {
            streams.cleanUp();
        }

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
