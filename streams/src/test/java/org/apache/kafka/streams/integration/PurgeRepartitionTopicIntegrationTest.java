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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@Category({IntegrationTest.class})
public class PurgeRepartitionTopicIntegrationTest {

    private static final int NUM_BROKERS = 1;

    private static final String INPUT_TOPIC = "input-stream";
    private static final String APPLICATION_ID = "restore-test";
    private static final String REPARTITION_TOPIC = APPLICATION_ID + "-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition";

    private static Admin adminClient;
    private static KafkaStreams kafkaStreams;
    private static final Integer PURGE_INTERVAL_MS = 10;
    private static final Integer PURGE_SEGMENT_BYTES = 2000;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, new Properties() {
        {
            put("log.retention.check.interval.ms", PURGE_INTERVAL_MS);
            put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, 0);
        }
    });

    @BeforeClass
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        CLUSTER.createTopic(INPUT_TOPIC, 1, 1);
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private final Time time = CLUSTER.time;

    private class RepartitionTopicCreatedWithExpectedConfigs implements TestCondition {
        @Override
        final public boolean conditionMet() {
            try {
                final Set<String> topics = adminClient.listTopics().names().get();

                if (!topics.contains(REPARTITION_TOPIC)) {
                    return false;
                }
            } catch (final Exception e) {
                return false;
            }

            try {
                final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, REPARTITION_TOPIC);
                final Config config = adminClient
                    .describeConfigs(Collections.singleton(resource))
                    .values()
                    .get(resource)
                    .get();
                return config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value().equals(TopicConfig.CLEANUP_POLICY_DELETE)
                        && config.get(TopicConfig.SEGMENT_MS_CONFIG).value().equals(PURGE_INTERVAL_MS.toString())
                        && config.get(TopicConfig.SEGMENT_BYTES_CONFIG).value().equals(PURGE_SEGMENT_BYTES.toString());
            } catch (final Exception e) {
                return false;
            }
        }
    }

    private interface TopicSizeVerifier {
        boolean verify(long currentSize);
    }

    private class RepartitionTopicVerified implements TestCondition {
        private final TopicSizeVerifier verifier;

        RepartitionTopicVerified(final TopicSizeVerifier verifier) {
            this.verifier = verifier;
        }

        @Override
        public final boolean conditionMet() {
            time.sleep(PURGE_INTERVAL_MS);

            try {
                final Collection<LogDirDescription> logDirInfo =
                    adminClient.describeLogDirs(Collections.singleton(0)).descriptions().get(0).get().values();

                for (final LogDirDescription partitionInfo : logDirInfo) {
                    final ReplicaInfo replicaInfo =
                        partitionInfo.replicaInfos().get(new TopicPartition(REPARTITION_TOPIC, 0));
                    if (replicaInfo != null && verifier.verify(replicaInfo.size())) {
                        return true;
                    }
                }
            } catch (final Exception e) {
                // swallow
            }

            return false;
        }
    }

    @Before
    public void setup() {
        // create admin client for verification
        final Properties adminConfig = new Properties();
        adminConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        adminClient = Admin.create(adminConfig);

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, PURGE_INTERVAL_MS);
        streamsConfiguration.put(StreamsConfig.REPARTITION_PURGE_INTERVAL_MS_CONFIG, PURGE_INTERVAL_MS);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(APPLICATION_ID).getPath());
        streamsConfiguration.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_MS_CONFIG), PURGE_INTERVAL_MS);
        streamsConfiguration.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG), PURGE_SEGMENT_BYTES);
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), PURGE_SEGMENT_BYTES / 2);    // we cannot allow batch size larger than segment size

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC)
               .groupBy(MockMapper.selectKeyKeyValueMapper())
               .count();

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration, time);
    }

    @After
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30));
        }
    }

    @Test
    public void shouldRestoreState() throws Exception {
        // produce some data to input topic
        final List<KeyValue<Integer, Integer>> messages = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            messages.add(new KeyValue<>(i, i));
        }
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC,
                messages,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                        IntegerSerializer.class,
                        IntegerSerializer.class),
                time.milliseconds());

        kafkaStreams.start();

        TestUtils.waitForCondition(new RepartitionTopicCreatedWithExpectedConfigs(), 60000,
                "Repartition topic " + REPARTITION_TOPIC + " not created with the expected configs after 60000 ms.");

        // wait until we received more than 1 segment of data, so that we can confirm the purge succeeds in next verification
        TestUtils.waitForCondition(
            new RepartitionTopicVerified(currentSize -> currentSize > PURGE_SEGMENT_BYTES),
            60000,
            "Repartition topic " + REPARTITION_TOPIC + " not received more than " + PURGE_SEGMENT_BYTES + "B of data after 60000 ms."
        );

        // we need long enough timeout to by-pass the log manager's InitialTaskDelayMs, which is hard-coded on server side
        TestUtils.waitForCondition(
            new RepartitionTopicVerified(currentSize -> currentSize <= PURGE_SEGMENT_BYTES),
            60000,
            "Repartition topic " + REPARTITION_TOPIC + " not purged data after 60000 ms."
        );
    }
}
