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
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StreamsStandByReplicaTest {

    private static final int KEY = 0;
    private static final int VALUE = 1;


    static final String SOURCE_TOPIC = "standbyTaskSource1";
    static final String SOURCE_TOPIC_2 = "standbyTaskSource2";

    private static final String SINK_TOPIC_1 = "standbyTaskSink1";
    private static final String SINK_TOPIC_2 = "standbyTaskSink2";


    public static void main(String[] args) {

        System.out.println("StreamsTest instance started");

        final String kafka = args.length > 0 ? args[0] : "localhost:9092";
        final String stateDirStr = args.length > 1 ? args[1] : TestUtils.tempDirectory().getAbsolutePath();
        final String additionalConfigs = args.length > 2 ? args[2] : null;

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-standby-tasks");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirStr);
        streamsProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // it is expected that max.poll.interval, retries, request.timeout and max.block.ms set
        // streams_broker_down_resilience_test and passed as args
        if (additionalConfigs != null && !additionalConfigs.equalsIgnoreCase("none")) {
            Map<String, String> updated = updatedConfigs(additionalConfigs);
            System.out.println("Updating configs with " + updated);
            streamsProperties.putAll(updated);
        }

        if (!confirmCorrectConfigs(streamsProperties)) {
            System.err.println(String.format("ERROR: Did not have all required configs expected  to contain %s %s %s %s",
                                             StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                                             StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG),
                                             StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                                             StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG)));

            System.exit(1);
        }

        final StreamsBuilder builder = new StreamsBuilder();

        String inMemoryStoreName = "in-memory-store";
        String persistentMemoryStoreName = "persistent-memory-store";

        KeyValueBytesStoreSupplier inMemoryStoreSupplier = Stores.inMemoryKeyValueStore(inMemoryStoreName);
        KeyValueBytesStoreSupplier persistentStoreSupplier = Stores.persistentKeyValueStore(persistentMemoryStoreName);

        KStream<String, String> inputStream = builder.stream(SOURCE_TOPIC, Consumed.with(stringSerde, stringSerde));

        ValueMapper<Long, String> countMapper = new ValueMapper<Long, String>() {
            @Override
            public String apply(Long value) {
                return value.toString();
            }
        };

        inputStream.groupByKey().count(Materialized.<String, Long>as(inMemoryStoreSupplier)).toStream().mapValues(countMapper).to(SINK_TOPIC_1, Produced.with(stringSerde,stringSerde));

        inputStream.groupByKey().count(Materialized.<String, Long>as(persistentStoreSupplier)).toStream().mapValues(countMapper).to(SINK_TOPIC_2, Produced.with(stringSerde,stringSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);

        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread t, final Throwable e) {
                System.err.println("FATAL: An unexpected exception " + e);
                e.printStackTrace(System.err);
                System.err.flush();
                shutdown(streams);
            }
        });

        streams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    Set<ThreadMetadata> threadMetadata = streams.localThreadsMetadata();
                    for (ThreadMetadata threadMetadatum : threadMetadata) {
                        System.out.println("ACTIVE_TASKS:" + threadMetadatum.activeTasks().size() + " STANDBY_TASKS:" + threadMetadatum.standbyTasks().size());
                    }
                }
            }
        });

        System.out.println("Start Kafka Streams");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutdown(streams);
                System.out.println("Shut down streams now");
            }
        }));


    }

    private static void shutdown(final KafkaStreams streams) {
        streams.close(10, TimeUnit.SECONDS);
    }

    private static boolean confirmCorrectConfigs(Properties properties) {
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
