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
package org.apache.kafka.streams.state;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstration application showing how to use TimestampedWindowStoreWithHeaders
 * in a real Kafka Streams application.
 *
 * This app:
 * 1. Reads events from "input-events" topic
 * 2. Stores them in a window store WITH headers
 * 3. Retrieves and processes based on headers
 * 4. Outputs results to "output-events" topic
 */
public class HeadersStoreDemo {

    public static void main(final String[] args) {
        System.out.println("\n╔════════════════════════════════════════════════════╗");
        System.out.println("║   Kafka Streams Headers Store Demo Application   ║");
        System.out.println("║             KIP-1271 Implementation               ║");
        System.out.println("╚════════════════════════════════════════════════════╝\n");

        final Properties props = getStreamsConfig();
        final StreamsBuilder builder = new StreamsBuilder();

        // Step 1: Create store supplier with headers support
        System.out.println("Creating store with headers support...");
        final WindowBytesStoreSupplier storeSupplier =
            Stores.persistentTimestampedWindowStoreWithHeaders(
                "user-events-store",
                Duration.ofHours(1),     // retention: 1 hour
                Duration.ofMinutes(5),   // window size: 5 minutes
                false                    // retainDuplicates
            );

        // Step 2: Create store builder
        final StoreBuilder<TimestampedWindowStoreWithHeaders<String, String>> storeBuilder =
            Stores.timestampedWindowStoreWithHeadersBuilder(
                storeSupplier,
                Serdes.String(),
                Serdes.String()
            );

        // Step 3: Add state store to topology
        builder.addStateStore(storeBuilder);
        System.out.println("✓ Store created: user-events-store");

        // Step 4: Build processing topology
        final KStream<String, String> inputStream = builder.stream("input-events");

        inputStream
            .process(() -> new EventProcessor("user-events-store"), "user-events-store")
            .to("output-events");

        System.out.println("✓ Topology built");
        System.out.println("\nTopology Description:");
        System.out.println(builder.build().describe());

        // Start the application
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // Add state listener to debug
        streams.setStateListener((newState, oldState) -> {
            System.out.println(">>> Kafka Streams state changed: " + oldState + " -> " + newState);
            if (newState == KafkaStreams.State.RUNNING) {
                System.out.println(">>> Application is now RUNNING and ready to consume!");
            }
        });

        // Add uncaught exception handler
        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println(">>> Uncaught exception occurred!");
            exception.printStackTrace();
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        // Attach shutdown handler
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("\n\nShutting down Kafka Streams application...");
                streams.close();
                latch.countDown();
            }
        });

        try {
            System.out.println("\n╔════════════════════════════════════════════════════╗");
            System.out.println("║          Application Starting...                  ║");
            System.out.println("║                                                    ║");
            System.out.println("║  Listening for events on: input-events            ║");
            System.out.println("║  Outputting results to: output-events             ║");
            System.out.println("║                                                    ║");
            System.out.println("║  Press Ctrl+C to stop                             ║");
            System.out.println("╚════════════════════════════════════════════════════╝\n");

            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.err.println("Error running application: " + e.getMessage());
            e.printStackTrace();
            Exit.exit(1);
        }
        Exit.exit(0);
    }

    /**
     * Processor that stores events with headers and processes them.
     */
    private static class EventProcessor extends ContextualProcessor<String, String, String, String> {
        private final String storeName;
        private TimestampedWindowStoreWithHeaders<String, String> store;

        public EventProcessor(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(final ProcessorContext<String, String> context) {
            super.init(context);
            store = context.getStateStore(storeName);
            System.out.println("✓ EventProcessor initialized with store: " + storeName);
        }

        @Override
        public void process(final Record<String, String> record) {
            final String key = record.key();
            final String value = record.value();
            final Headers headers = record.headers();
            final long timestamp = record.timestamp();

            System.out.println("\n" + "=".repeat(60));
            System.out.println("📥 INCOMING EVENT");
            System.out.println("=".repeat(60));
            System.out.println("Key: " + key);
            System.out.println("Value: " + value);
            System.out.println("Timestamp: " + timestamp + " (" + new java.util.Date(timestamp) + ")");
            System.out.println("Headers:");
            headers.forEach(h ->
                System.out.println("  • " + h.key() + " = " + new String(h.value(), StandardCharsets.UTF_8))
            );

            // ✅ WRITE: Store event with headers
            final long windowStart = (timestamp / 300000) * 300000; // 5-minute windows
            store.put(key, value, windowStart, timestamp, headers);
            System.out.println("\n✓ Stored in window [" + new java.util.Date(windowStart) + "]");

            // ✅ READ: Retrieve from store and process based on headers
            System.out.println("\n📖 READING FROM STORE:");
            final WindowStoreIterator<ValueTimestampHeaders<String>> iterator =
                store.fetch(key, windowStart, windowStart + 300000);

            int count = 0;
            while (iterator.hasNext()) {
                final KeyValue<Long, ValueTimestampHeaders<String>> kv = iterator.next();
                final ValueTimestampHeaders<String> stored = kv.value;

                // ✅ Direct access to value, timestamp, and headers!
                final String storedValue = stored.value();
                final long storedTimestamp = stored.timestamp();
                final Headers storedHeaders = stored.headers();

                count++;
                System.out.println("  Record #" + count + ":");
                System.out.println("    Value: " + storedValue);
                System.out.println("    Timestamp: " + new java.util.Date(storedTimestamp));

                // Process based on headers
                String output = processBasedOnHeaders(storedValue, storedHeaders);

                System.out.println("    Processing result: " + output);

                // Forward to output topic
                context().forward(record.withValue(output));
            }
            iterator.close();

            System.out.println("✓ Found " + count + " record(s) in window");
            System.out.println("=".repeat(60) + "\n");
        }

        /**
         * Example of conditional processing based on headers.
         */
        private String processBasedOnHeaders(final String value, final Headers headers) {
            final StringBuilder result = new StringBuilder();
            result.append(value);

            // Check schema version
            if (headers.lastHeader("schema-version") != null) {
                final String version = new String(
                    headers.lastHeader("schema-version").value(),
                    StandardCharsets.UTF_8
                );
                result.append(" [schema:").append(version).append("]");

                if ("v2".equals(version)) {
                    result.append(" [USING_V2_PROCESSOR]");
                } else if ("v1".equals(version)) {
                    result.append(" [USING_V1_PROCESSOR]");
                }
            }

            // Check device type
            if (headers.lastHeader("device-type") != null) {
                final String device = new String(
                    headers.lastHeader("device-type").value(),
                    StandardCharsets.UTF_8
                );
                result.append(" [device:").append(device).append("]");

                if ("mobile".equals(device)) {
                    result.append(" [MOBILE_OPTIMIZED]");
                }
            }

            // Check region
            if (headers.lastHeader("region") != null) {
                final String region = new String(
                    headers.lastHeader("region").value(),
                    StandardCharsets.UTF_8
                );
                result.append(" [region:").append(region).append("]");
            }

            return result.toString();
        }
    }

    /**
     * Configuration for Kafka Streams.
     */
    private static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "headers-store-demo-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // State store configuration
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-headers-demo");

        // For demo purposes - commit frequently
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // Start from earliest offset for demo purposes
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }
}
