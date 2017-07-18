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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Properties;

@Category({IntegrationTest.class})
public class StateRestorationIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_MESSAGES = 3;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private final MockTime mockTime = CLUSTER.time;
    private final String topicName = "restoreTestTopic";
    private final MockStateRestoreListener reportingRestoreListener = new MockStateRestoreListener();
    private final AllMessagesReceivedCondition messagesReceivedCondition = new AllMessagesReceivedCondition();

    private KStreamBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private KStream<Long, String> kStream;
    private int messageCounter = 0;


    @Before
    public void setUp() throws Exception {
        messageCounter = 0;
        builder = new KStreamBuilder();
        CLUSTER.createTopic(topicName);
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "restoration-integration-app");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        builder.addStateStore(Stores.create("store").withLongKeys().withStringValues().persistent().build());
        kStream = builder.stream(Serdes.Long(), Serdes.String(), topicName);

        kStream.process(new ProcessorSupplier<Long, String>() {
            @Override
            public Processor<Long, String> get() {
                return new AbstractProcessor<Long, String>() {
                    KeyValueStore<Long, String> store;

                    @Override
                    @SuppressWarnings("unchecked")
                    public void init(ProcessorContext context) {
                        store = (KeyValueStore<Long, String>) context.getStateStore("store");
                    }

                    @Override
                    public void process(Long key, String value) {
                        messageCounter++;
                        store.put(key, value);
                    }
                };
            }
        }, "store");


    }

    @After
    public void tearDown() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }


    @Test
    public void shouldReportRestorationCalls() throws Exception {
        startStreams();
        produceTopicValues();

        TestUtils.waitForCondition(messagesReceivedCondition, 30000L, "Failed to read messages");

        closeAndCleanUpStreams();
        startStreams();

        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return reportingRestoreListener.restoreStartOffset == 0L &&
                       reportingRestoreListener.restoredBatchOffset == 3L &&
                       reportingRestoreListener.restoreEndOffset == 3L;
            }
        }, 30000L, "Failed to read restored states");

        produceTopicValues();

        TestUtils.waitForCondition(messagesReceivedCondition, 30000L, "Failed to read messages in second load");

        closeAndCleanUpStreams();
        startStreams();

        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return reportingRestoreListener.restoreStartOffset == 0L &&
                       reportingRestoreListener.restoredBatchOffset == 6L &&
                       reportingRestoreListener.restoreEndOffset == 6L;
            }
        }, 30000L, "Failed to read restored states in second reload");

    }

    private void closeAndCleanUpStreams() {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
        messageCounter = 0;
    }

    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder, streamsConfiguration);
        kafkaStreams.setStateRestoreListener(reportingRestoreListener);
        kafkaStreams.start();
    }


    private void produceTopicValues() throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronously(
            topicName,
            Arrays.asList(
                new KeyValue<>(1L, "foo"),
                new KeyValue<>(2L, "bar"),
                new KeyValue<>(3L, "baz")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                LongSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime);
    }


    private class AllMessagesReceivedCondition implements TestCondition {

        @Override
        public boolean conditionMet() {
            return messageCounter >= NUM_MESSAGES;
        }
    }


}
