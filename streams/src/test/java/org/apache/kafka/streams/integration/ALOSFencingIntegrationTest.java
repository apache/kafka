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

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public class ALOSFencingIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(ALOSFencingIntegrationTest.class);

    private String appId;
    private String inputTopic;
    private String counterName;

    private String outputTopic;

    private KafkaStreams streamInstanceOne;
    private KafkaStreams streamInstanceTwo;
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
        counterName = "counter-" + safeTestName;

        CLUSTER.deleteTopicsAndWait(inputTopic, outputTopic);
        CLUSTER.createTopic(inputTopic, 1, 3);
        CLUSTER.createTopic(outputTopic, 1, 3);
    }

    @After
    public void cleanUp() {
        if (streamInstanceOne != null) {
            streamInstanceOne.close();
        }
        if (streamInstanceTwo != null) {
            streamInstanceTwo.close();
        }
    }

    // The test produces a duplicate fee range of integers from 0 (inclusive) to initialBulk as input to the stream.
    // The topology will want to count the messages in the state and forward the messages as is.
    // The changelog topic of the counter should contain a monotonically increasing sequence of values, otherwise any following rebalance
    // may restore a value that did not count values.
    @Test
    public void shouldRespectALOSWithZombieThreads() throws Exception {
        final long time = System.currentTimeMillis();
        final String base = TestUtils.tempDirectory(appId).getPath();

        final int initialBulk = 100;

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

        streamInstanceOne = buildLaggingCounterTopology(base + "-1", 1, initialBulk);
        streamInstanceTwo = buildLaggingCounterTopology(base + "-2", 2, initialBulk);

        LOG.info("Start first instance and wait for it to hang");
        startApplicationAndWaitUntilRunning(Collections.singletonList(streamInstanceOne), Duration.ofSeconds(30));
        IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                UUID.randomUUID().toString(),
                IntegerDeserializer.class,
                IntegerDeserializer.class
            ),
            outputTopic,
            initialBulk / 2
        );
        LOG.info("Almost finished reading the initialBulk");

        LOG.info("Start second instance to take over processing");
        startApplicationAndWaitUntilRunning(Collections.singletonList(streamInstanceTwo), Duration.ofSeconds(30));

        LOG.info("Wait for the processing to be completed");
        IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                UUID.randomUUID().toString(),
                IntegerDeserializer.class,
                IntegerDeserializer.class
            ),
            outputTopic,
            initialBulk,
            Duration.ofMinutes(10L).toMillis()
        );
        LOG.info("Processing completed");

        Thread.sleep(1500);
        LOG.info("Trying to read");

        final List<ConsumerRecord<Integer, Integer>> changelogRecords = IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                UUID.randomUUID().toString(),
                IntegerDeserializer.class,
                IntegerDeserializer.class
            ),
            appId + "-" + counterName + "-changelog",
            1,
            Duration.ofMinutes(10L).toMillis()
        );

        changelogRecords.stream().forEach(this::log);

        // Our changelog should be monotonically increasing, or we risk dataloss if a restoration happens.
        ConsumerRecord<Integer, Integer> last = null;
        for (final ConsumerRecord<Integer, Integer> t : changelogRecords) {
            if (last != null) {
                assertThat(String.format("%s %s", last.value().toString(), t.value().toString()), last.value().compareTo(t.value()) <= 0);
            }
            last = t;
        }
    }

    private void log(final ConsumerRecord<Integer, Integer> record) {
        LOG.info("changelog record: " + record);
    }

    private void logIfDuplicate(final Integer id, final List<ConsumerRecord<Integer, Integer>> record) {
        assertThat("The id and the value in the records must match", record.stream().allMatch(r -> id.equals(r.value())));
        if (record.size() > 1) {
            LOG.warn(
                "Id : " + id + " is assigned to the following " + record.stream().map(ConsumerRecord::key).collect(Collectors.toList()));
        }
    }

    private KafkaStreams buildLaggingCounterTopology(final String stateDirPath, final int whoAmI, final int initialBulk) {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(counterName),
            Serdes.Integer(),
            Serdes.Integer()).withCachingEnabled()
        );
        builder.<Integer, Integer>stream(inputTopic)
            .process(
                () -> new Processor<Integer, Integer, Integer, Integer>() {
                    private KeyValueStore<Integer, Integer> counter;
                    private ProcessorContext<Integer, Integer> context;

                    @Override
                    public void init(final ProcessorContext<Integer, Integer> context) {
                        this.context = context;
                        counter = context.getStateStore(counterName);
                    }

                    @Override
                    public void process(final Record<Integer, Integer> record) {
                        final Integer key = record.key();

                        final int counterKey = 0;
                        final Integer lastCounter = counter.get(counterKey);
                        final int newCounter = lastCounter != null ? lastCounter + 1 : 0;
                        counter.put(counterKey, newCounter);
                        if (whoAmI == 1 && newCounter == initialBulk / 2) {
                            // Let's fall out of the consumer group and then continue processing
                            try {
                                Thread.sleep(1000);
                            } catch (final InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        context.forward(record.withKey(key));
                    }

                    @Override
                    public void close() {
                    }
                },
                counterName
            )
            .to(outputTopic);
        return new KafkaStreams(builder.build(), props(stateDirPath));
    }


    private Properties props(final String stateDirPath) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        streamsConfiguration.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1L);
        streamsConfiguration.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), 500);
        streamsConfiguration.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, Long.MAX_VALUE);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }
}
