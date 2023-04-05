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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Category(IntegrationTest.class)
public class StandbyTaskEOSMultiRebalanceIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(StandbyTaskEOSMultiRebalanceIntegrationTest.class);

    private final static long TWO_MINUTE_TIMEOUT = Duration.ofMinutes(2L).toMillis();

    private String appId;
    private String inputTopic;
    private String storeName;
    private String counterName;

    private String outputTopic;

    private KafkaStreams streamInstanceOne;
    private KafkaStreams streamInstanceTwo;
    private KafkaStreams streamInstanceThree;

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Before
    public void createTopics() throws Exception {
        final String safeTestName = UUID.randomUUID().toString();
        appId = "app-" + safeTestName;
        inputTopic = "input-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        storeName = "store-" + safeTestName;
        counterName = "counter-" + safeTestName;

        CLUSTER.deleteTopicsAndWait(inputTopic, outputTopic);
        CLUSTER.createTopic(inputTopic, partitionCount, 3);
        CLUSTER.createTopic(outputTopic, partitionCount, 3);
    }

    private final int partitionCount = 12;

    @After
    public void cleanUp() {
        if (streamInstanceOne != null) {
            streamInstanceOne.close();
        }
        if (streamInstanceTwo != null) {
            streamInstanceTwo.close();
        }
        if (streamInstanceThree != null) {
            streamInstanceThree.close();
        }
    }

    // The test produces a duplicate fee range of integers from 0 (inclusive) to initialBulk + secondBulk (exclusive) as input to the stream.
    // The stream is responsible for assigning a unique id to each of the integers.
    // The output topic must thus contain 63000 message:
    //      The Key is unique and from the range of input values
    //      The Values produced are unique.
    @Test
    public void shouldHonorEOSWhenUsingCachingAndStandbyReplicas() throws Exception {
        final Properties readCommitted = new Properties();
        readCommitted.setProperty("isolation.level", "read_committed");
        final long time = System.currentTimeMillis();
        final String base = TestUtils.tempDirectory(appId).getPath();

        final int initialBulk = 3000;
        final int secondBulk = 60000;

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic,
                IntStream.range(0, initialBulk).boxed().map(i -> new KeyValue<>(i, i)).collect(Collectors.toList()),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        IntegerSerializer.class,
                        IntegerSerializer.class,
                        new Properties()
                ),
                10L + time
        );

        streamInstanceOne = buildWithUniqueIdAssignmentTopology(base + "-1");
        streamInstanceTwo = buildWithUniqueIdAssignmentTopology(base + "-2");
        streamInstanceThree = buildWithUniqueIdAssignmentTopology(base + "-3");

        LOG.info("start first instance and wait for completed processing");
        startApplicationAndWaitUntilRunning(Collections.singletonList(streamInstanceOne), Duration.ofSeconds(30));
        IntegrationTestUtils.waitUntilMinRecordsReceived(
                TestUtils.consumerConfig(
                        CLUSTER.bootstrapServers(),
                        UUID.randomUUID().toString(),
                        IntegerDeserializer.class,
                        IntegerDeserializer.class,
                        readCommitted
                ),
                outputTopic,
                initialBulk
        );
        LOG.info("Finished reading the initial bulk");

        LOG.info("start second instance and wait for standby replication");
        startApplicationAndWaitUntilRunning(Collections.singletonList(streamInstanceTwo), Duration.ofSeconds(30));
        waitForCondition(
                () -> streamInstanceTwo.store(
                        StoreQueryParameters.fromNameAndType(
                                storeName,
                                QueryableStoreTypes.<Integer, Integer>keyValueStore()
                        ).enableStaleStores()
                ).get(0) != null,
                TWO_MINUTE_TIMEOUT,
                "Could not get key from standby store"
        );
        LOG.info("Second stream have some data in the state store");


        LOG.info("Produce the second bulk");
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic,
                IntStream.range(initialBulk, initialBulk + secondBulk).boxed().map(i -> new KeyValue<>(i, i)).collect(Collectors.toList()),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        IntegerSerializer.class,
                        IntegerSerializer.class,
                        new Properties()
                ),
                1000L + time
        );

        LOG.info("Start stream three which will introduce a re-balancing event and hopefully some redistribution of tasks.");
        startApplicationAndWaitUntilRunning(Collections.singletonList(streamInstanceThree), Duration.ofSeconds(90));

        LOG.info("Wait for the processing to be completed");
        final List<ConsumerRecord<Integer, Integer>> outputRecords = IntegrationTestUtils.waitUntilMinRecordsReceived(
                TestUtils.consumerConfig(
                        CLUSTER.bootstrapServers(),
                        UUID.randomUUID().toString(),
                        IntegerDeserializer.class,
                        IntegerDeserializer.class,
                        readCommitted
                ),
                outputTopic,
                initialBulk + secondBulk,
                Duration.ofMinutes(10L).toMillis()
        );
        LOG.info("Processing completed");

        outputRecords.stream().collect(Collectors.groupingBy(ConsumerRecord::value)).forEach(this::logIfDuplicate);

        assertThat("Each output should correspond to one distinct value", outputRecords.stream().map(ConsumerRecord::value).distinct().count(), is(Matchers.equalTo((long) outputRecords.size())));
    }

    private void logIfDuplicate(final Integer id, final List<ConsumerRecord<Integer, Integer>> record) {
        assertThat("The id and the value in the records must match", record.stream().allMatch(r -> id.equals(r.value())));
        if (record.size() > 1) {
            LOG.warn("Id : " + id + " is assigned to the following " + record.stream().map(ConsumerRecord::key).collect(Collectors.toList()));
        }
    }

    private KafkaStreams buildWithUniqueIdAssignmentTopology(final String stateDirPath) {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.Integer(),
                Serdes.Integer())
        );
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(counterName),
                Serdes.Integer(),
                Serdes.Integer()).withCachingEnabled()
        );
        builder.<Integer, Integer>stream(inputTopic)
                .process(
                        () -> new Processor<Integer, Integer, Integer, Integer>() {
                            private KeyValueStore<Integer, Integer> store;
                            private KeyValueStore<Integer, Integer> counter;
                            private ProcessorContext<Integer, Integer> context;

                            @Override
                            public void init(final ProcessorContext<Integer, Integer> context) {
                                this.context = context;
                                store = context.getStateStore(storeName);
                                counter = context.getStateStore(counterName);
                            }

                            @Override
                            public void process(final Record<Integer, Integer> record) {
                                final Integer key = record.key();
                                final Integer unused = record.value();
                                assertThat("Key and value mus be equal", key.equals(unused));
                                Integer id = store.get(key);
                                // Only assign a new id if the value have not been observed before
                                if (id == null) {
                                    final int counterKey = 0;
                                    final Integer lastCounter = counter.get(counterKey);
                                    final int newCounter = lastCounter == null ? 0 : lastCounter + 1;
                                    counter.put(counterKey, newCounter);
                                    // Partitions assign ids from their own id space
                                    id = newCounter * partitionCount + context.recordMetadata().get().partition();
                                    store.put(key, id);
                                }
                                context.forward(record.withKey(id));
                            }

                            @Override
                            public void close() {
                            }
                        },
                        storeName, counterName
                )
                .to(outputTopic);
        return new KafkaStreams(builder.build(), props(stateDirPath));
    }


    private Properties props(final String stateDirPath) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

        streamsConfiguration.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }
}
