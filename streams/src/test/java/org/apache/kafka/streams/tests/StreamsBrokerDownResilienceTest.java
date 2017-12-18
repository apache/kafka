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
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.test.TestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StreamsBrokerDownResilienceTest {

    private static final int KEY = 0;
    private static final int VALUE = 1;

    private static final String SOURCE_TOPIC_1 = "streamsResilienceSource";

    private static final String SINK_TOPIC = "streamsResilienceSink";

    public static void main(String[] args) {

        System.out.println("StreamsTest instance started");

        final String kafka = args.length > 0 ? args[0] : "localhost:9092";
        final String stateDirStr = args.length > 1 ? args[1] : TestUtils.tempDirectory().getAbsolutePath();
        final String additionalConfigs = args.length > 2 ? args[2] : null;

        final Serde<String> stringSerde = Serdes.String();

        final Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-resilience");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);


        // it is expected that max.poll.interval, retries, request.timeout and max.block.ms set
        // streams_broker_down_resilience_test and passed as args
        if (additionalConfigs != null && !additionalConfigs.equalsIgnoreCase("none")) {
            System.out.println("Updating configs with " + additionalConfigs);
            streamsProperties.putAll(updatedConfigs(additionalConfigs));
        }

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(Collections.singletonList(SOURCE_TOPIC_1), Consumed.with(stringSerde, stringSerde))
            .peek(new ForeachAction<String, String>() {
                @Override
                public void apply(String key, String value) {
                    System.out.println("received key " + key + " and value " + value);
                }
            }).to(SINK_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);

        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread t, final Throwable e) {
                System.err.println("FATAL: An unexpected exception " + e);
                System.err.flush();
                streams.close(30, TimeUnit.SECONDS);
            }
        });
        System.out.println("Start Kafka Streams");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Shutting down streams now");
                streams.close(10, TimeUnit.SECONDS);
            }
        }));


    }

    /**
     * Takes a string with keys and values separated by '=' and each key value pair
     * separated by ',' for example max.block.ms=5000,retries=6,request.timeout.ms=6000
     *
     * @param formattedConfigs the formatted config string
     * @return HashMap with keys and values inserted
     */
    private static Map<String, String> updatedConfigs(String formattedConfigs) {
        String[] parts = formattedConfigs.split(",");
        Map<String, String> updatedConfigs = new HashMap<>();
        for (String part : parts) {
            String[] keyValue = part.split("=");
            updatedConfigs.put(keyValue[KEY], keyValue[VALUE]);
        }
        return updatedConfigs;
    }

}
