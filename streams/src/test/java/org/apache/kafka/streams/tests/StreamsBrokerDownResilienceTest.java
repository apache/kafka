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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamsBrokerDownResilienceTest {

    private static final int KEY = 0;
    private static final int VALUE = 1;

    private static final String SOURCE_TOPIC_1 = "streamsResilienceSource";

    private static final String SINK_TOPIC = "streamsResilienceSink";

    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("StreamsBrokerDownResilienceTest are expecting two parameters: propFile, additionalConfigs; but only see " + args.length + " parameter");
            Exit.exit(1);
        }

        System.out.println("StreamsTest instance started");

        final String propFileName = args[0];
        final String additionalConfigs = args[1];

        final Properties streamsProperties = Utils.loadProps(propFileName);
        final String kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            Exit.exit(1);
        }

        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-resilience");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);


        // it is expected that max.poll.interval, retries, request.timeout and max.block.ms set
        // streams_broker_down_resilience_test and passed as args
        if (additionalConfigs != null && !additionalConfigs.equalsIgnoreCase("none")) {
            final Map<String, String> updated = updatedConfigs(additionalConfigs);
            System.out.println("Updating configs with " + updated);
            streamsProperties.putAll(updated);
        }

        if (!confirmCorrectConfigs(streamsProperties)) {
            System.err.println(String.format("ERROR: Did not have all required configs expected  to contain %s %s %s %s",
                                             StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                                             StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG),
                                             StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                                             StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG)));

            Exit.exit(1);
        }

        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();

        builder.stream(Collections.singletonList(SOURCE_TOPIC_1), Consumed.with(stringSerde, stringSerde))
            .peek(new ForeachAction<String, String>() {
                int messagesProcessed = 0;
                @Override
                public void apply(final String key, final String value) {
                    System.out.println("received key " + key + " and value " + value);
                    messagesProcessed++;
                    System.out.println("processed " + messagesProcessed + " messages");
                    System.out.flush();
                }
            }).to(SINK_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);

        streams.setUncaughtExceptionHandler(e -> {
                System.err.println("FATAL: An unexpected exception " + e);
                System.err.flush();
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        );

        System.out.println("Start Kafka Streams");
        streams.start();

        Exit.addShutdownHook("streams-shutdown-hook", () -> {
            streams.close(Duration.ofSeconds(30));
            System.out.println("Complete shutdown of streams resilience test app now");
            System.out.flush();
        });
    }

    private static boolean confirmCorrectConfigs(final Properties properties) {
        return properties.containsKey(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) &&
               properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG)) &&
               properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)) &&
               properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    /**
     * Takes a string with keys and values separated by '=' and each key value pair
     * separated by ',' for example max.block.ms=5000,retries=6,request.timeout.ms=6000
     *
     * @param formattedConfigs the formatted config string
     * @return HashMap with keys and values inserted
     */
    private static Map<String, String> updatedConfigs(final String formattedConfigs) {
        final String[] parts = formattedConfigs.split(",");
        final Map<String, String> updatedConfigs = new HashMap<>();
        for (final String part : parts) {
            final String[] keyValue = part.split("=");
            updatedConfigs.put(keyValue[KEY], keyValue[VALUE]);
        }
        return updatedConfigs;
    }

}
