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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({IntegrationTest.class})
public class RackAwarenessIntegrationTest {
    private static final int NUM_BROKERS = 1;

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final String TAG_VALUE_K8_CLUSTER_1 = "k8s-cluster-1";
    private static final String TAG_VALUE_K8_CLUSTER_2 = "k8s-cluster-2";
    private static final String TAG_VALUE_K8_CLUSTER_3 = "k8s-cluster-3";
    private static final String TAG_VALUE_EU_CENTRAL_1A = "eu-central-1a";
    private static final String TAG_VALUE_EU_CENTRAL_1B = "eu-central-1b";
    private static final String TAG_VALUE_EU_CENTRAL_1C = "eu-central-1c";

    private static final int DEFAULT_NUMBER_OF_STATEFUL_SUB_TOPOLOGIES = 1;
    private static final int DEFAULT_NUMBER_OF_PARTITIONS_OF_SUB_TOPOLOGIES = 2;

    @Rule
    public TestName testName = new TestName();

    private static final String INPUT_TOPIC = "input-topic";

    private static final String TAG_ZONE = "zone";
    private static final String TAG_CLUSTER = "cluster";

    private List<KafkaStreamsWithConfiguration> kafkaStreamsInstances;
    private Properties baseConfiguration;
    private Topology topology;

    @BeforeClass
    public static void createTopics() throws Exception {
        CLUSTER.start();
        CLUSTER.createTopic(INPUT_TOPIC, 6, 1);
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
    public void shouldDoRebalancingWithMaximumNumberOfClientTags() throws Exception {
        initTopology(3, 3);
        final int numberOfStandbyReplicas = 1;

        final List<String> clientTagKeys = new ArrayList<>();
        final Map<String, String> clientTags1 = new HashMap<>();
        final Map<String, String> clientTags2 = new HashMap<>();

        for (int i = 0; i < StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE; i++) {
            clientTagKeys.add("key-" + i);
        }

        for (int i = 0; i < clientTagKeys.size(); i++) {
            final String key = clientTagKeys.get(i);
            clientTags1.put(key, "value-1-" + i);
            clientTags2.put(key, "value-2-" + i);
        }

        assertEquals(StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE, clientTagKeys.size());
        Stream.of(clientTags1, clientTags2)
              .forEach(clientTags -> assertEquals(String.format("clientsTags with content '%s' " +
                                                                "did not match expected size", clientTags),
                                                  StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE,
                                                  clientTags.size()));

        createAndStart(clientTags1, clientTagKeys, numberOfStandbyReplicas);
        createAndStart(clientTags1, clientTagKeys, numberOfStandbyReplicas);
        createAndStart(clientTags2, clientTagKeys, numberOfStandbyReplicas);

        waitUntilAllKafkaStreamsClientsAreRunning();
        assertTrue(isIdealTaskDistributionReachedForTags(clientTagKeys));

        stopKafkaStreamsInstanceWithIndex(0);

        waitUntilAllKafkaStreamsClientsAreRunning();

        assertTrue(isIdealTaskDistributionReachedForTags(clientTagKeys));
    }

    @Test
    public void shouldDistributeStandbyReplicasWhenAllClientsAreLocatedOnASameClusterTag() throws Exception {
        initTopology();
        final int numberOfStandbyReplicas = 1;
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1A, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1A, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1A, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1B, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1B, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1B, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        waitUntilAllKafkaStreamsClientsAreRunning();
        assertTrue(isIdealTaskDistributionReachedForTags(singletonList(TAG_ZONE)));
    }

    @Test
    public void shouldDistributeStandbyReplicasOverMultipleClientTags() throws Exception {
        initTopology();
        final int numberOfStandbyReplicas = 2;

        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1A, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1B, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1C, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1A, TAG_VALUE_K8_CLUSTER_2), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1B, TAG_VALUE_K8_CLUSTER_2), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1C, TAG_VALUE_K8_CLUSTER_2), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1A, TAG_VALUE_K8_CLUSTER_3), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1B, TAG_VALUE_K8_CLUSTER_3), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1C, TAG_VALUE_K8_CLUSTER_3), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        waitUntilAllKafkaStreamsClientsAreRunning();
        assertTrue(isIdealTaskDistributionReachedForTags(asList(TAG_ZONE, TAG_CLUSTER)));
    }

    @Test
    public void shouldDistributeStandbyReplicasWhenIdealDistributionCanNotBeAchieved() throws Exception {
        initTopology();
        final int numberOfStandbyReplicas = 2;

        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1A, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1B, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1C, TAG_VALUE_K8_CLUSTER_1), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1A, TAG_VALUE_K8_CLUSTER_2), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1B, TAG_VALUE_K8_CLUSTER_2), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(buildClientTags(TAG_VALUE_EU_CENTRAL_1C, TAG_VALUE_K8_CLUSTER_2), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        waitUntilAllKafkaStreamsClientsAreRunning();

        assertTrue(isIdealTaskDistributionReachedForTags(singletonList(TAG_ZONE)));
        assertTrue(isPartialTaskDistributionReachedForTags(singletonList(TAG_CLUSTER)));
    }

    private void stopKafkaStreamsInstanceWithIndex(final int index) {
        kafkaStreamsInstances.get(index).kafkaStreams.close(Duration.ofMillis(IntegrationTestUtils.DEFAULT_TIMEOUT));
        kafkaStreamsInstances.remove(index);
    }

    private void waitUntilAllKafkaStreamsClientsAreRunning() throws Exception {
        waitUntilAllKafkaStreamsClientsAreRunning(Duration.ofMillis(IntegrationTestUtils.DEFAULT_TIMEOUT));
    }

    private void waitUntilAllKafkaStreamsClientsAreRunning(final Duration timeout) throws Exception {
        IntegrationTestUtils.waitForApplicationState(kafkaStreamsInstances.stream().map(it -> it.kafkaStreams).collect(Collectors.toList()),
                                                     KafkaStreams.State.RUNNING,
                                                     timeout);
    }

    private boolean isPartialTaskDistributionReachedForTags(final Collection<String> tagsToCheck) {
        final Predicate<TaskClientTagDistribution> partialTaskClientTagDistributionTest = taskClientTagDistribution -> {
            final Map<String, String> activeTaskClientTags = taskClientTagDistribution.activeTaskClientTags.clientTags;
            return tagsAmongstActiveAndAtLeastOneStandbyTaskIsDifferent(taskClientTagDistribution.standbyTasksClientTags, activeTaskClientTags, tagsToCheck);
        };

        return isTaskDistributionTestSuccessful(partialTaskClientTagDistributionTest);
    }

    private boolean isIdealTaskDistributionReachedForTags(final Collection<String> tagsToCheck) {
        final Predicate<TaskClientTagDistribution> idealTaskClientTagDistributionTest = taskClientTagDistribution -> {
            final Map<String, String> activeTaskClientTags = taskClientTagDistribution.activeTaskClientTags.clientTags;
            return tagsAmongstStandbyTasksAreDifferent(taskClientTagDistribution.standbyTasksClientTags, tagsToCheck)
                   && tagsAmongstActiveAndAllStandbyTasksAreDifferent(taskClientTagDistribution.standbyTasksClientTags,
                                                                      activeTaskClientTags,
                                                                      tagsToCheck);
        };

        return isTaskDistributionTestSuccessful(idealTaskClientTagDistributionTest);
    }

    private boolean isTaskDistributionTestSuccessful(final Predicate<TaskClientTagDistribution> taskClientTagDistributionPredicate) {
        final List<TaskClientTagDistribution> tasksClientTagDistributions = getTasksClientTagDistributions();

        if (tasksClientTagDistributions.isEmpty()) {
            return false;
        }

        return tasksClientTagDistributions.stream().allMatch(taskClientTagDistributionPredicate);
    }

    private static boolean tagsAmongstActiveAndAllStandbyTasksAreDifferent(final Collection<TaskClientTags> standbyTasks,
                                                                           final Map<String, String> activeTaskClientTags,
                                                                           final Collection<String> tagsToCheck) {
        return standbyTasks.stream().allMatch(standbyTask -> tagsToCheck.stream().noneMatch(tag -> activeTaskClientTags.get(tag).equals(standbyTask.clientTags.get(tag))));
    }

    private static boolean tagsAmongstActiveAndAtLeastOneStandbyTaskIsDifferent(final Collection<TaskClientTags> standbyTasks,
                                                                                final Map<String, String> activeTaskClientTags,
                                                                                final Collection<String> tagsToCheck) {
        return standbyTasks.stream().anyMatch(standbyTask -> tagsToCheck.stream().noneMatch(tag -> activeTaskClientTags.get(tag).equals(standbyTask.clientTags.get(tag))));
    }

    private static boolean tagsAmongstStandbyTasksAreDifferent(final Collection<TaskClientTags> standbyTasks, final Collection<String> tagsToCheck) {
        final Map<String, Integer> statistics = new HashMap<>();

        for (final TaskClientTags standbyTask : standbyTasks) {
            for (final String tag : tagsToCheck) {
                final String tagValue = standbyTask.clientTags.get(tag);
                final Integer tagValueOccurrence = statistics.getOrDefault(tagValue, 0);
                statistics.put(tagValue, tagValueOccurrence + 1);
            }
        }

        return statistics.values().stream().noneMatch(occurrence -> occurrence > 1);
    }

    private void initTopology() {
        initTopology(DEFAULT_NUMBER_OF_PARTITIONS_OF_SUB_TOPOLOGIES, DEFAULT_NUMBER_OF_STATEFUL_SUB_TOPOLOGIES);
    }

    private void initTopology(final int numberOfPartitionsOfSubTopologies, final int numberOfStatefulSubTopologies) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String stateStoreName = "myTransformState";

        final StoreBuilder<KeyValueStore<Integer, Integer>> keyValueStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName),
            Serdes.Integer(),
            Serdes.Integer()
        );

        builder.addStateStore(keyValueStoreBuilder);

        final KStream<Integer, Integer> stream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.Integer()));

        // Stateless sub-topology
        stream.repartition(Repartitioned.numberOfPartitions(numberOfPartitionsOfSubTopologies)).filter((k, v) -> true);

        // Stateful sub-topologies
        for (int i = 0; i < numberOfStatefulSubTopologies; i++) {
            stream.repartition(Repartitioned.numberOfPartitions(numberOfPartitionsOfSubTopologies))
                  .groupByKey()
                  .reduce(Integer::sum);
        }

        topology = builder.build();
    }

    private List<TaskClientTagDistribution> getTasksClientTagDistributions() {
        final List<TaskClientTagDistribution> taskClientTags = new ArrayList<>();

        for (final KafkaStreamsWithConfiguration kafkaStreamsInstance : kafkaStreamsInstances) {
            final StreamsConfig config = new StreamsConfig(kafkaStreamsInstance.configuration);
            for (final ThreadMetadata localThreadsMetadata : kafkaStreamsInstance.kafkaStreams.metadataForLocalThreads()) {
                localThreadsMetadata.activeTasks().forEach(activeTask -> {
                    final TaskId activeTaskId = activeTask.taskId();
                    final Map<String, String> clientTags = config.getClientTags();

                    final List<TaskClientTags> standbyTasks = findStandbysForActiveTask(activeTaskId);

                    if (!standbyTasks.isEmpty()) {
                        final TaskClientTags activeTaskView = new TaskClientTags(activeTaskId, clientTags);
                        taskClientTags.add(new TaskClientTagDistribution(activeTaskView, standbyTasks));
                    }
                });

            }
        }

        return taskClientTags;
    }

    private List<TaskClientTags> findStandbysForActiveTask(final TaskId taskId) {
        final List<TaskClientTags> standbyTasks = new ArrayList<>();

        for (final KafkaStreamsWithConfiguration kafkaStreamsInstance : kafkaStreamsInstances) {
            for (final ThreadMetadata localThreadsMetadata : kafkaStreamsInstance.kafkaStreams.metadataForLocalThreads()) {
                localThreadsMetadata.standbyTasks().forEach(standbyTask -> {
                    final TaskId standbyTaskId = standbyTask.taskId();
                    if (taskId.equals(standbyTaskId)) {
                        final StreamsConfig config = new StreamsConfig(kafkaStreamsInstance.configuration);
                        standbyTasks.add(new TaskClientTags(standbyTaskId, config.getClientTags()));
                    }
                });
            }
        }

        return standbyTasks;
    }

    private static Map<String, String> buildClientTags(final String zone, final String cluster) {
        final Map<String, String> clientTags = new HashMap<>();

        clientTags.put(TAG_ZONE, zone);
        clientTags.put(TAG_CLUSTER, cluster);

        return clientTags;
    }

    private void createAndStart(final Map<String, String> clientTags,
                                final Collection<String> rackAwareAssignmentTags,
                                final int numberOfStandbyReplicas) {
        final Properties streamsConfiguration = createStreamsConfiguration(clientTags, rackAwareAssignmentTags, numberOfStandbyReplicas);
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfiguration);

        kafkaStreamsInstances.add(new KafkaStreamsWithConfiguration(streamsConfiguration, kafkaStreams));

        kafkaStreams.start();
    }

    private Properties createStreamsConfiguration(final Map<String, String> clientTags,
                                                  final Collection<String> rackAwareAssignmentTags,
                                                  final int numStandbyReplicas) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.putAll(baseConfiguration);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
        streamsConfiguration.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, String.join(",", rackAwareAssignmentTags));
        clientTags.forEach((key, value) -> streamsConfiguration.put(StreamsConfig.clientTagPrefix(key), value));
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(String.join("-", clientTags.values())).getPath());
        return streamsConfiguration;
    }

    private static final class KafkaStreamsWithConfiguration {
        private final Properties configuration;
        private final KafkaStreams kafkaStreams;

        KafkaStreamsWithConfiguration(final Properties configuration, final KafkaStreams kafkaStreams) {
            this.configuration = configuration;
            this.kafkaStreams = kafkaStreams;
        }
    }

    private static final class TaskClientTagDistribution {
        private final TaskClientTags activeTaskClientTags;
        private final List<TaskClientTags> standbyTasksClientTags;

        TaskClientTagDistribution(final TaskClientTags activeTaskClientTags, final List<TaskClientTags> standbyTasksClientTags) {
            this.activeTaskClientTags = activeTaskClientTags;
            this.standbyTasksClientTags = standbyTasksClientTags;
        }

        @Override
        public String toString() {
            return "TaskDistribution{" +
                   "activeTaskClientTagsView=" + activeTaskClientTags +
                   ", standbyTasks=" + standbyTasksClientTags +
                   '}';
        }
    }

    private static final class TaskClientTags {
        private final TaskId taskId;
        private final Map<String, String> clientTags;

        TaskClientTags(final TaskId taskId, final Map<String, String> clientTags) {
            this.taskId = taskId;
            this.clientTags = clientTags;
        }

        @Override
        public String toString() {
            return "TaskClientTags{" +
                   "taskId=" + taskId +
                   ", clientTags=" + clientTags +
                   '}';
        }
    }
}