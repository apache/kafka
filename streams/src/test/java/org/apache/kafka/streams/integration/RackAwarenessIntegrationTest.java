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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;

@Category({IntegrationTest.class})
public class RackAwarenessIntegrationTest {
    public static final TaskId TASK_0_0 = new TaskId(0, 0);
    public static final TaskId TASK_0_1 = new TaskId(0, 1);
    public static final TaskId TASK_0_2 = new TaskId(0, 2);
    public static final TaskId TASK_0_3 = new TaskId(0, 3);
    public static final TaskId TASK_0_4 = new TaskId(0, 4);
    public static final TaskId TASK_0_5 = new TaskId(0, 5);
    public static final TaskId TASK_0_6 = new TaskId(0, 6);
    public static final TaskId TASK_1_0 = new TaskId(1, 0);
    public static final TaskId TASK_1_1 = new TaskId(1, 1);
    public static final TaskId TASK_1_2 = new TaskId(1, 2);
    public static final TaskId TASK_1_3 = new TaskId(1, 3);
    public static final TaskId TASK_2_0 = new TaskId(2, 0);
    public static final TaskId TASK_2_1 = new TaskId(2, 1);
    public static final TaskId TASK_2_2 = new TaskId(2, 2);
    public static final TaskId TASK_2_3 = new TaskId(2, 3);

    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @Rule
    public TestName testName = new TestName();

    private static final String INPUT_TOPIC = "input-topic";

    private List<KafkaStreamsWithConfiguration> kafkaStreamsInstances;
    private Properties baseConfiguration;

    @BeforeClass
    public static void createTopics() throws Exception {
        CLUSTER.start();
        CLUSTER.createTopic(INPUT_TOPIC, 1, 1);
    }

    @Before
    public void setup() {
        kafkaStreamsInstances = new ArrayList<>();
        baseConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final String applicationId = "app-" + safeTestName;
        baseConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        baseConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        baseConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        baseConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        baseConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
    }

    @After
    public void cleanup() throws IOException {
        for (final KafkaStreamsWithConfiguration kafkaStreamsWithConfiguration : kafkaStreamsInstances) {
            kafkaStreamsWithConfiguration.kafkaStreams.close(Duration.ofMillis(IntegrationTestUtils.DEFAULT_TIMEOUT));
            IntegrationTestUtils.purgeLocalStreamsState(kafkaStreamsWithConfiguration.configuration);
        }
        kafkaStreamsInstances.clear();
    }

    @Test
    public void simpleTest() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        final String stateStoreName = "myTransformState";
        final StoreBuilder<KeyValueStore<Integer, Integer>> keyValueStoreBuilder =
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                                        Serdes.Integer(),
                                        Serdes.Integer());
        builder.addStateStore(keyValueStoreBuilder);
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.Integer()))
               .transform(() -> new Transformer<Integer, Integer, KeyValue<Integer, Integer>>() {
                   @SuppressWarnings("unchecked")
                   @Override
                   public void init(final ProcessorContext context) {
                   }

                   @Override
                   public KeyValue<Integer, Integer> transform(final Integer key, final Integer value) {
                       return null;
                   }

                   @Override
                   public void close() {
                   }
               }, stateStoreName);

        final Topology topology = builder.build();

        final KafkaStreams kStream1ACluster1 = createAndStart(topology, streamsConfiguration(buildClientTagMap("eu-central-1a", "k8s-cluster-1"), "zone,cluster", 1));
        final KafkaStreams kStream1BCluster1 = createAndStart(topology, streamsConfiguration(buildClientTagMap("eu-central-1b", "k8s-cluster-1"), "zone,cluster", 1));
        final KafkaStreams kStream1CCluster1 = createAndStart(topology, streamsConfiguration(buildClientTagMap("eu-central-1c", "k8s-cluster-1"), "zone,cluster", 1));

        waitUntilBothClientAreOK(
            "At least one client did not reach state RUNNING with active tasks but no stand-by tasks",
            () -> {
                final boolean b = hasActiveTasks(kStream1ACluster1);
                final boolean b1 = hasActiveTasks(kStream1BCluster1);
                final boolean b2 = hasActiveTasks(kStream1CCluster1);
                return b || b1 || b2;
            }
        );
    }

    private static Map<String, String> buildClientTagMap(final String zone, final String cluster) {
        final Map<String, String> clientTags = new HashMap<>();

        clientTags.put("zone", zone);
        clientTags.put("cluster", cluster);

        return clientTags;
    }

    private boolean hasActiveTasks(final KafkaStreams kafkaStreams) {
        final Set<ThreadMetadata> threadMetadata1 = kafkaStreams.localThreadsMetadata();
        return threadMetadata1.stream().anyMatch(threadMetadata -> !threadMetadata.activeTasks().isEmpty());
    }

    private boolean hasStandbyTasks(final KafkaStreams kafkaStreams) {
        return kafkaStreams.localThreadsMetadata().stream().anyMatch(threadMetadata -> !threadMetadata.standbyTasks().isEmpty());
    }

    private void waitUntilBothClientAreOK(final String message, final TestCondition testCondition) throws Exception {
        TestUtils.waitForCondition(testCondition, IntegrationTestUtils.DEFAULT_TIMEOUT, message);
    }

    private KafkaStreams createAndStart(final Topology topology,
                                        final Properties streamsConfigs) {
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfigs);

        kafkaStreamsInstances.add(new KafkaStreamsWithConfiguration(streamsConfigs, kafkaStreams));

        kafkaStreams.start();

        return kafkaStreams;
    }

    private Properties streamsConfiguration(final Map<String, String> clientTags,
                                            final String rackAwareAssignmentTags,
                                            final int numStandbyReplicas) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.putAll(baseConfiguration);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
        streamsConfiguration.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, rackAwareAssignmentTags);
        clientTags.forEach((key, value) -> streamsConfiguration.put(StreamsConfig.clientTagPrefix(key), value));
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(String.join("-", clientTags.values())).getPath());
        return streamsConfiguration;
    }

    private static class KafkaStreamsWithConfiguration {
        private final Properties configuration;
        private final KafkaStreams kafkaStreams;

        KafkaStreamsWithConfiguration(final Properties configuration, final KafkaStreams kafkaStreams) {
            this.configuration = configuration;
            this.kafkaStreams = kafkaStreams;
        }
    }
}
