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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Minimal long-running Kafka Streams application used by the topology description
 * plugin system tests. Reads {@value #SOURCE_TOPIC}, filters out null values, writes
 * to {@value #SINK_TOPIC}. Runs until killed by SIGTERM.
 */
public class TopologyDescriptionPluginSystemTest {

    public static final String SOURCE_TOPIC = "topology-description-source";
    public static final String SINK_TOPIC = "topology-description-sink";
    public static final String APPLICATION_ID = "kafka-streams-topology-description-plugin-system-test";

    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: TopologyDescriptionPluginSystemTest <propFile>");
            Exit.exit(1);
        }

        System.out.println("StreamsTest instance started");

        final Properties streamsProperties = Utils.loadProps(args[0]);
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(SOURCE_TOPIC)
            .filter((key, value) -> value != null)
            .to(SINK_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        System.out.println("start Kafka Streams");
        streams.start();
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("Kafka Streams stopped");
    }
}
