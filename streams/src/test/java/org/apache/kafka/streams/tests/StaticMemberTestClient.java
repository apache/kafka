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
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Objects;
import java.util.Properties;

public class StaticMemberTestClient {

    private static final String TEST_NAME = "StaticMemberTestClient";

    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println(TEST_NAME + " requires one argument (properties-file) but none provided: ");
        }

        System.out.println("StreamsTest instance started");

        final String propFileName = args[0];

        final Properties streamsProperties = Utils.loadProps(propFileName);

        final String groupInstanceId = Objects.requireNonNull(streamsProperties.getProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));

        System.out.println(TEST_NAME + " instance started with group.instance.id " + groupInstanceId);
        System.out.println("props=" + streamsProperties);
        System.out.flush();

        final StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = (String) (Objects.requireNonNull(streamsProperties.remove("input.topic")));

        final KStream<String, String> dataStream = builder.stream(inputTopic);
        dataStream.peek((k, v) ->  System.out.printf("PROCESSED key=%s value=%s%n", k, v));

        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, TEST_NAME);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        config.putAll(streamsProperties);

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.setStateListener((newState, oldState) -> {
            if (oldState == KafkaStreams.State.REBALANCING && newState == KafkaStreams.State.RUNNING) {
                System.out.println("REBALANCING -> RUNNING");
                System.out.flush();
            }
        });

        streams.start();

        Exit.addShutdownHook("streams-shutdown-hook", () -> {
            System.out.println("closing Kafka Streams instance");
            System.out.flush();
            streams.close();
            System.out.println("Static membership test closed");
            System.out.flush();
        });
    }
}
