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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

public class StreamsUpgradeTest {

    /**
     * This test cannot be run executed, as long as Kafka 0.11.0.3 is not released
     */
    @SuppressWarnings("unchecked")
    public static void main(final String[] args) {
        if (args.length < 2) {
            System.err.println("StreamsUpgradeTest requires three argument (kafka-url, state-dir, [upgradeFrom: optional]) but only " + args.length + " provided: "
                + (args.length > 0 ? args[0] : ""));
        }
        final String kafka = args[0];
        final String stateDir = args[1];
        final String upgradeFrom = args.length > 2 ? args[2] : null;

        System.out.println("StreamsTest instance started (StreamsUpgradeTest v0.11.0)");
        System.out.println("kafka=" + kafka);
        System.out.println("stateDir=" + stateDir);
        System.out.println("upgradeFrom=" + upgradeFrom);

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream dataStream = builder.stream("data");
        dataStream.process(printProcessorSupplier());
        dataStream.to("echo");

        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsUpgradeTest");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        config.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        if (upgradeFrom != null) {
            // TODO: because Kafka 0.11.0.3 is not released yet, thus `UPGRADE_FROM_CONFIG` is not available yet
            //config.setProperty(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFrom);
            config.setProperty("upgrade.from", upgradeFrom);
        }

        final KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                streams.close();
                System.out.println("UPGRADE-TEST-CLIENT-CLOSED");
                System.out.flush();
            }
        });
    }

    private static <K, V> ProcessorSupplier<K, V> printProcessorSupplier() {
        return new ProcessorSupplier<K, V>() {
            public Processor<K, V> get() {
                return new AbstractProcessor<K, V>() {
                    private int numRecordsProcessed = 0;

                    @Override
                    public void init(final ProcessorContext context) {
                        System.out.println("initializing processor: topic=data taskId=" + context.taskId());
                        numRecordsProcessed = 0;
                    }

                    @Override
                    public void process(final K key, final V value) {
                        numRecordsProcessed++;
                        if (numRecordsProcessed % 100 == 0) {
                            System.out.println("processed " + numRecordsProcessed + " records from topic=data");
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
}
