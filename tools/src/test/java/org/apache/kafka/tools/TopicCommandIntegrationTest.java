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

package org.apache.kafka.tools;

import kafka.admin.RackAwareTest;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.utils.Logging;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@Tag("integration")
@SuppressWarnings("deprecation") // Added for Scala 2.12 compatibility for usages of JavaConverters
public class TopicCommandIntegrationTest extends kafka.integration.KafkaServerTestHarness implements Logging, RackAwareTest {
    private short defaultReplicationFactor = 1;
    private int numPartitions = 1;
    private TopicCommand.TopicService topicService;
    private Admin adminClient;
    private String bootstrapServer;
    private String testTopicName;
    private long defaultTimeout = 10000;

    /**
     * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
     * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
     *
     * Note the replica fetch max bytes is set to `1` in order to throttle the rate of replication for test
     * `testDescribeUnderReplicatedPartitionsWhenReassignmentIsInProgress`.
     */
    @Override
    public scala.collection.Seq<KafkaConfig> generateConfigs() {
        Map<Integer, String> rackInfo = new HashMap<>();
        rackInfo.put(0, "rack1");
        rackInfo.put(1, "rack2");
        rackInfo.put(2, "rack2");
        rackInfo.put(3, "rack1");
        rackInfo.put(4, "rack3");
        rackInfo.put(5, "rack3");

        List<Properties> brokerConfigs = ToolsTestUtils
            .createBrokerProperties(6, zkConnectOrNull(), rackInfo, numPartitions, defaultReplicationFactor);

        List<KafkaConfig> configs = new ArrayList<>();
        for (Properties props : brokerConfigs) {
            props.put(KafkaConfig.ReplicaFetchMaxBytesProp(), "1");
            configs.add(KafkaConfig.fromProps(props));
        }
        return JavaConverters.asScalaBuffer(configs).toSeq();
    }

    private TopicCommand.TopicCommandOptions buildTopicCommandOptionsWithBootstrap(String... opts) {
        String[] finalOptions = Stream.concat(Arrays.asList(opts).stream(),
                Arrays.asList("--bootstrap-server", bootstrapServer).stream()
        ).toArray(String[]::new);
        return new TopicCommand.TopicCommandOptions(finalOptions);
    }

    private void createAndWaitTopic(TopicCommand.TopicCommandOptions opts) throws Exception {
        topicService.createTopic(opts);
        waitForTopicCreated(opts.topic().get());
    }

    private void waitForTopicCreated(String topicName) {
        waitForTopicCreated(topicName, defaultTimeout);
    }

    private void waitForTopicCreated(String topicName, long timeout) {
        TestUtils.waitForPartitionMetadata(brokers(), topicName, 0, timeout);
    }

    @BeforeEach
    public void setUp(TestInfo info) {
        super.setUp(info);
        // create adminClient
        Properties props = new Properties();
        bootstrapServer = bootstrapServers(listenerName());
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        adminClient = Admin.create(props);
        topicService = new TopicCommand.TopicService(props, Optional.of(bootstrapServer));
        testTopicName = String.format("%s-%s", info.getTestMethod().get().getName(), TestUtils.randomString(10));
    }
    @AfterEach
    public void close() throws Exception {
        if (topicService != null)
            topicService.close();
        if (adminClient != null)
            adminClient.close();
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreate(String quorum) throws Exception {
        createAndWaitTopic(buildTopicCommandOptionsWithBootstrap(
            "--create", "--partitions", "2", "--replication-factor", "1", "--topic", testTopicName));

        assertTrue(adminClient.listTopics().names().get().contains(testTopicName));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithDefaults(String quorum) throws Exception {
        createAndWaitTopic(buildTopicCommandOptionsWithBootstrap("--create", "--topic", testTopicName));

        List<TopicPartitionInfo> partitions = adminClient
            .describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames()
            .get()
            .get(testTopicName)
            .partitions();
        assertEquals(numPartitions, partitions.size());
        assertEquals(defaultReplicationFactor, (short) partitions.get(0).replicas().size());
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithDefaultReplication(String quorum) throws Exception {
        createAndWaitTopic(buildTopicCommandOptionsWithBootstrap("--create", "--topic", testTopicName, "--partitions", "2"));

        List<TopicPartitionInfo>  partitions = adminClient
            .describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames()
            .get()
            .get(testTopicName)
            .partitions();
        assertEquals(2, partitions.size());
        assertEquals(defaultReplicationFactor, (short) partitions.get(0).replicas().size());
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithDefaultPartitions(String quorum) throws Exception {
        createAndWaitTopic(buildTopicCommandOptionsWithBootstrap("--create", "--topic", testTopicName, "--replication-factor", "2"));

        List<TopicPartitionInfo> partitions = adminClient
            .describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames()
            .get()
            .get(testTopicName)
            .partitions();

        assertEquals(numPartitions, partitions.size());
        assertEquals(2, (short) partitions.get(0).replicas().size());
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithConfigs(String quorum) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        createAndWaitTopic(buildTopicCommandOptionsWithBootstrap("--create", "--partitions", "2", "--replication-factor", "2", "--topic", testTopicName, "--config",
                "delete.retention.ms=1000"));

        Config configs = adminClient.describeConfigs(Collections.singleton(configResource)).all().get().get(configResource);
        assertEquals(1000, Integer.valueOf(configs.get("delete.retention.ms").value()));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWhenAlreadyExists(String quorum) throws Exception {
        int numPartitions = 1;

        // create the topic
        TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap(
            "--create", "--partitions", Integer.toString(numPartitions), "--replication-factor", "1",
                "--topic", testTopicName);
        createAndWaitTopic(createOpts);

        // try to re-create the topic
        assertThrows(TopicExistsException.class, () -> topicService.createTopic(createOpts));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWhenAlreadyExistsWithIfNotExists(String quorum) throws Exception {
        TopicCommand.TopicCommandOptions createOpts =
                buildTopicCommandOptionsWithBootstrap("--create", "--topic", testTopicName, "--if-not-exists");
        createAndWaitTopic(createOpts);
        topicService.createTopic(createOpts);
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithReplicaAssignment(String quorum) throws Exception {
        // create the topic
        TopicCommand.TopicCommandOptions createOpts =
                buildTopicCommandOptionsWithBootstrap("--create", "--replica-assignment", "5:4,3:2,1:0", "--topic", testTopicName);
        createAndWaitTopic(createOpts);

        List<TopicPartitionInfo> partitions = adminClient
            .describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames()
            .get()
            .get(testTopicName)
            .partitions();
        
        assertEquals(3, partitions.size());
        assertEquals(Arrays.asList(5, 4), getPartitionReplicas(partitions, 0));
        assertEquals(Arrays.asList(3, 2), getPartitionReplicas(partitions, 1));
        assertEquals(Arrays.asList(1, 0), getPartitionReplicas(partitions, 2));
    }

    private List<Integer> getPartitionReplicas(List<TopicPartitionInfo> partitions, int partitionNumber) {
        return partitions.get(partitionNumber).replicas().stream().map(Node::id).collect(Collectors.toList());
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithInvalidReplicationFactor(String quorum) {
        TopicCommand.TopicCommandOptions opts = buildTopicCommandOptionsWithBootstrap("--create", "--partitions", "2", "--replication-factor", Integer.toString(Short.MAX_VALUE + 1),
            "--topic", testTopicName);
        assertThrows(IllegalArgumentException.class, () -> topicService.createTopic(opts));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithNegativeReplicationFactor(String quorum) {
        TopicCommand.TopicCommandOptions opts = buildTopicCommandOptionsWithBootstrap("--create",
            "--partitions", "2", "--replication-factor", "-1", "--topic", testTopicName);
        assertThrows(IllegalArgumentException.class, () -> topicService.createTopic(opts));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithNegativePartitionCount(String quorum) {
        TopicCommand.TopicCommandOptions opts = buildTopicCommandOptionsWithBootstrap("--create", "--partitions", "-1", "--replication-factor", "1", "--topic", testTopicName);
        assertThrows(IllegalArgumentException.class, () -> topicService.createTopic(opts));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testInvalidTopicLevelConfig(String quorum) {
        TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap("--create",
            "--partitions", "1", "--replication-factor", "1", "--topic", testTopicName,
            "--config", "message.timestamp.type=boom");
        assertThrows(ConfigException.class, () -> topicService.createTopic(createOpts));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testListTopics(String quorum) throws Exception {
        createAndWaitTopic(buildTopicCommandOptionsWithBootstrap(
            "--create", "--partitions", "1", "--replication-factor", "1", "--topic", testTopicName));

        String output = captureListTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--list"));
        assertTrue(output.contains(testTopicName));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testListTopicsWithIncludeList(String quorum) throws ExecutionException, InterruptedException {
        String topic1 = "kafka.testTopic1";
        String topic2 = "kafka.testTopic2";
        String topic3 = "oooof.testTopic1";
        adminClient.createTopics(
                Arrays.asList(new NewTopic(topic1, 2, (short) 2),
                    new NewTopic(topic2, 2, (short) 2),
                    new NewTopic(topic3, 2, (short) 2)))
            .all().get();
        waitForTopicCreated(topic1);
        waitForTopicCreated(topic2);
        waitForTopicCreated(topic3);

        String output = captureListTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--list", "--topic", "kafka.*"));

        assertTrue(output.contains(topic1));
        assertTrue(output.contains(topic2));
        assertFalse(output.contains(topic3));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testListTopicsWithExcludeInternal(String quorum) throws ExecutionException, InterruptedException {
        String topic1 = "kafka.testTopic1";
        adminClient.createTopics(
                Arrays.asList(new NewTopic(topic1, 2, (short) 2),
                    new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, 2, (short) 2)))
            .all().get();
        waitForTopicCreated(topic1);

        String output = captureListTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--list", "--exclude-internal"));

        assertTrue(output.contains(topic1));
        assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterPartitionCount(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            Arrays.asList(new NewTopic(testTopicName, 2, (short) 2))).all().get();
        waitForTopicCreated(testTopicName);

        topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter", "--topic", testTopicName, "--partitions", "3"));

        kafka.utils.TestUtils.waitUntilTrue(
            () -> brokers().forall(b -> b.metadataCache().getTopicPartitions(testTopicName).size() == 3),
            () -> "Timeout waiting for new assignment propagating to broker", org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);
        TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues().get(testTopicName).get();
        assertEquals(3, topicDescription.partitions().size());
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterAssignment(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            Collections.singletonList(new NewTopic(testTopicName, 2, (short) 2))).all().get();
        waitForTopicCreated(testTopicName);

        topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter",
            "--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "3"));
        kafka.utils.TestUtils.waitUntilTrue(
            () -> brokers().forall(b -> b.metadataCache().getTopicPartitions(testTopicName).size() == 3),
            () -> "Timeout waiting for new assignment propagating to broker",
            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);

        TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues().get(testTopicName).get();
        assertTrue(topicDescription.partitions().size() == 3);
        List<Integer> partitionReplicas = getPartitionReplicas(topicDescription.partitions(), 2);
        assertEquals(Arrays.asList(4, 2), partitionReplicas);
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterAssignmentWithMoreAssignmentThanPartitions(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            Arrays.asList(new NewTopic(testTopicName, 2, (short) 2))).all().get();
        waitForTopicCreated(testTopicName);

        assertThrows(ExecutionException.class,
            () -> topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter",
                "--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2,3:2", "--partitions", "3")));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterAssignmentWithMorePartitionsThanAssignment(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            Arrays.asList(new NewTopic(testTopicName, 2, (short) 2))).all().get();
        waitForTopicCreated(testTopicName);

        assertThrows(ExecutionException.class,
            () -> topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter", "--topic", testTopicName,
                "--replica-assignment", "5:3,3:1,4:2", "--partitions", "6")));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterWithInvalidPartitionCount(String quorum) throws Exception {
        createAndWaitTopic(
                buildTopicCommandOptionsWithBootstrap("--create", "--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)
        );

        assertThrows(ExecutionException.class,
            () -> topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter", "--partitions", "-1", "--topic", testTopicName)));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterWhenTopicDoesntExist(String quorum) {
        // alter a topic that does not exist without --if-exists
        TopicCommand.TopicCommandOptions alterOpts = buildTopicCommandOptionsWithBootstrap("--alter", "--topic", testTopicName, "--partitions", "1");
        TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient);
        assertThrows(IllegalArgumentException.class, () -> topicService.alterTopic(alterOpts));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterWhenTopicDoesntExistWithIfExists(String quorum) throws ExecutionException, InterruptedException {
        topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter", "--topic", testTopicName, "--partitions", "1", "--if-exists"));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateAlterTopicWithRackAware(String quorum) throws Exception {
        Map<Integer, String> rackInfo = new HashMap<Integer, String>();
        rackInfo.put(0, "rack1");
        rackInfo.put(1, "rack2");
        rackInfo.put(2, "rack2");
        rackInfo.put(3, "rack1");
        rackInfo.put(4, "rack3");
        rackInfo.put(5, "rack3");

        int numPartitions = 18;
        int replicationFactor = 3;
        TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap("--create",
            "--partitions", Integer.toString(numPartitions),
            "--replication-factor", Integer.toString(replicationFactor),
            "--topic", testTopicName);
        createAndWaitTopic(createOpts);

        Map<Integer, List<Integer>> assignment = adminClient.describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames().get().get(testTopicName).partitions()
            .stream()
            .collect(Collectors.toMap(
                info -> info.partition(),
                info -> info.replicas().stream().map(Node::id).collect(Collectors.toList())));
        checkReplicaDistribution(assignment, rackInfo, rackInfo.size(), numPartitions,
            replicationFactor, true, true, true);

        int alteredNumPartitions = 36;
        // verify that adding partitions will also be rack aware
        TopicCommand.TopicCommandOptions alterOpts = buildTopicCommandOptionsWithBootstrap("--alter",
            "--partitions", Integer.toString(alteredNumPartitions),
            "--topic", testTopicName);
        topicService.alterTopic(alterOpts);

        kafka.utils.TestUtils.waitUntilTrue(
            () -> brokers().forall(p -> p.metadataCache().getTopicPartitions(testTopicName).size() == alteredNumPartitions),
            () -> "Timeout waiting for new assignment propagating to broker",
            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);

        assignment = adminClient.describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames().get().get(testTopicName).partitions().stream()
            .collect(Collectors.toMap(info -> info.partition(), info -> info.replicas().stream().map(Node::id).collect(Collectors.toList())));
        checkReplicaDistribution(assignment, rackInfo, rackInfo.size(), alteredNumPartitions, replicationFactor,
            true, true, true);
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testConfigPreservationAcrossPartitionAlteration(String quorum) throws Exception {
        int numPartitionsOriginal = 1;
        String cleanupKey = "cleanup.policy";
        String cleanupVal = "compact";

        // create the topic
        TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap("--create",
            "--partitions", Integer.toString(numPartitionsOriginal),
            "--replication-factor", "1",
            "--config", cleanupKey + "=" + cleanupVal,
            "--topic", testTopicName);
        createAndWaitTopic(createOpts);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Config props = adminClient.describeConfigs(Collections.singleton(configResource)).all().get().get(configResource);
        // val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, testTopicName)
        assertNotNull(props.get(cleanupKey), "Properties after creation don't contain " + cleanupKey);
        assertEquals(cleanupVal, props.get(cleanupKey).value(), "Properties after creation have incorrect value");

        // pre-create the topic config changes path to avoid a NoNodeException
        if (!isKRaftTest()) {
            zkClient().makeSurePersistentPathExists(kafka.zk.ConfigEntityChangeNotificationZNode.path());
        }

        // modify the topic to add new partitions
        int numPartitionsModified = 3;
        TopicCommand.TopicCommandOptions alterOpts = buildTopicCommandOptionsWithBootstrap("--alter",
            "--partitions", Integer.toString(numPartitionsModified), "--topic", testTopicName);
        topicService.alterTopic(alterOpts);
        Config newProps = adminClient.describeConfigs(Collections.singleton(configResource)).all().get().get(configResource);
        assertNotNull(newProps.get(cleanupKey), "Updated properties do not contain " + cleanupKey);
        assertEquals(cleanupVal, newProps.get(cleanupKey).value(), "Updated properties have incorrect value");
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testTopicDeletion(String quorum) throws Exception {
        // create the NormalTopic
        TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap("--create",
            "--partitions", "1",
            "--replication-factor", "1",
            "--topic", testTopicName);
        createAndWaitTopic(createOpts);

        // delete the NormalTopic
        TopicCommand.TopicCommandOptions deleteOpts = buildTopicCommandOptionsWithBootstrap("--delete", "--topic", testTopicName);

        if (!isKRaftTest()) {
            String deletePath = kafka.zk.DeleteTopicsTopicZNode.path(testTopicName);
            assertFalse(zkClient().pathExists(deletePath), "Delete path for topic shouldn't exist before deletion.");
        }
        topicService.deleteTopic(deleteOpts);
        TestUtils.verifyTopicDeletion(zkClientOrNull(), testTopicName, 1, brokers());
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testTopicWithCollidingCharDeletionAndCreateAgain(String quorum) throws Exception {
        // create the topic with colliding chars
        String topicWithCollidingChar = "test.a";
        TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap("--create",
            "--partitions", "1",
            "--replication-factor", "1",
            "--topic", topicWithCollidingChar);
        createAndWaitTopic(createOpts);

        // delete the topic
        TopicCommand.TopicCommandOptions deleteOpts = buildTopicCommandOptionsWithBootstrap("--delete", "--topic", topicWithCollidingChar);

        if (!isKRaftTest()) {
            String deletePath = kafka.zk.DeleteTopicsTopicZNode.path(topicWithCollidingChar);
            assertFalse(zkClient().pathExists(deletePath), "Delete path for topic shouldn't exist before deletion.");
        }
        topicService.deleteTopic(deleteOpts);
        TestUtils.verifyTopicDeletion(zkClientOrNull(), topicWithCollidingChar, 1, brokers());
        assertDoesNotThrow(() -> createAndWaitTopic(createOpts));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteInternalTopic(String quorum) throws Exception {
        // create the offset topic
        TopicCommand.TopicCommandOptions createOffsetTopicOpts = buildTopicCommandOptionsWithBootstrap("--create",
            "--partitions", "1",
            "--replication-factor", "1",
            "--topic", Topic.GROUP_METADATA_TOPIC_NAME);
        createAndWaitTopic(createOffsetTopicOpts);

        // Try to delete the Topic.GROUP_METADATA_TOPIC_NAME which is allowed by default.
        // This is a difference between the new and the old command as the old one didn't allow internal topic deletion.
        // If deleting internal topics is not desired, ACLS should be used to control it.
        TopicCommand.TopicCommandOptions deleteOffsetTopicOpts =
                buildTopicCommandOptionsWithBootstrap("--delete", "--topic", Topic.GROUP_METADATA_TOPIC_NAME);
        String deleteOffsetTopicPath = kafka.zk.DeleteTopicsTopicZNode.path(Topic.GROUP_METADATA_TOPIC_NAME);
        if (!isKRaftTest()) {
            assertFalse(zkClient().pathExists(deleteOffsetTopicPath), "Delete path for topic shouldn't exist before deletion.");
        }
        topicService.deleteTopic(deleteOffsetTopicOpts);
        TestUtils.verifyTopicDeletion(zkClientOrNull(), Topic.GROUP_METADATA_TOPIC_NAME, 1, brokers());
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteWhenTopicDoesntExist(String quorum) {
        // delete a topic that does not exist
        TopicCommand.TopicCommandOptions deleteOpts = buildTopicCommandOptionsWithBootstrap("--delete", "--topic", testTopicName);
        assertThrows(IllegalArgumentException.class, () -> topicService.deleteTopic(deleteOpts));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteWhenTopicDoesntExistWithIfExists(String quorum) throws ExecutionException, InterruptedException {
        topicService.deleteTopic(buildTopicCommandOptionsWithBootstrap("--delete", "--topic", testTopicName, "--if-exists"));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribe(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            Collections.singletonList(new NewTopic(testTopicName, 2, (short) 2))).all().get();
        waitForTopicCreated(testTopicName);

        String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName));
        String[] rows = output.split("\n");
        assertEquals(3, rows.length);
        assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeWhenTopicDoesntExist(String quorum) {
        assertThrows(IllegalArgumentException.class,
            () -> topicService.describeTopic(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName)));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeWhenTopicDoesntExistWithIfExists(String quorum) throws ExecutionException, InterruptedException {
        topicService.describeTopic(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName, "--if-exists"));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnavailablePartitions(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            Collections.singletonList(new NewTopic(testTopicName, 6, (short) 1))).all().get();
        waitForTopicCreated(testTopicName);

        try {
            // check which partition is on broker 0 which we'll kill
            TopicDescription testTopicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName))
                .allTopicNames().get().get(testTopicName);
            int partitionOnBroker0 = testTopicDescription.partitions().stream()
                .filter(partition -> partition.leader().id() == 0)
                .findFirst().get().partition();

            killBroker(0);

            // wait until the topic metadata for the test topic is propagated to each alive broker
            kafka.utils.TestUtils.waitUntilTrue(
                () -> {
                    boolean result = true;
                    for (KafkaBroker server : JavaConverters.asJavaCollection(brokers())) {
                        if (server.config().brokerId() != 0) {
                            Set<String> topicNames = Collections.singleton(testTopicName);
                            Collection<MetadataResponseData.MetadataResponseTopic> topicMetadatas =
                                JavaConverters.asJavaCollection(server.dataPlaneRequestProcessor().metadataCache()
                                .getTopicMetadata(JavaConverters.asScalaSetConverter(topicNames).asScala().toSet(),
                                    ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                                    false, false)
                                );
                            Optional<MetadataResponseData.MetadataResponseTopic> testTopicMetadata = topicMetadatas.stream()
                                .filter(metadata -> metadata.name().equals(testTopicName))
                                .findFirst();
                            if (!testTopicMetadata.isPresent()) {
                                throw new AssertionError("Topic metadata is not found in metadata cache");
                            }
                            Optional<MetadataResponseData.MetadataResponsePartition> testPartitionMetadata = testTopicMetadata.get().partitions().stream()
                                .filter(metadata -> metadata.partitionIndex() == partitionOnBroker0)
                                .findFirst();
                            if (!testPartitionMetadata.isPresent()) {
                                throw new AssertionError("Partition metadata is not found in metadata cache");
                            }
                            result = result && testPartitionMetadata.get().errorCode() == Errors.LEADER_NOT_AVAILABLE.code();
                        }
                    }
                    return result;
                },
                () -> String.format("Partition metadata for %s is not propagated", testTopicName),
                org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);

            // grab the console output and assert
            String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName, "--unavailable-partitions"));
            String[] rows = output.split("\n");
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)));
            assertTrue(rows[0].contains("Leader: none\tReplicas: 0\tIsr:"));
        } finally {
            restartDeadBrokers(false);
        }
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnderReplicatedPartitions(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            Collections.singletonList(new NewTopic(testTopicName, 1, (short) 6))).all().get();
        waitForTopicCreated(testTopicName);

        try {
            killBroker(0);
            if (isKRaftTest()) {
                ensureConsistentKRaftMetadata();
            } else {
                TestUtils.waitForPartitionMetadata(aliveBrokers(), testTopicName, 0, org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS);
            }
            String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--under-replicated-partitions"));
            String[] rows = output.split("\n");
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)), String.format("Unexpected output: %s", rows[0]));
        } finally {
            restartDeadBrokers(false);
        }
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnderMinIsrPartitions(String quorum) throws ExecutionException, InterruptedException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6");

        adminClient.createTopics(
            Collections.singletonList(new NewTopic(testTopicName, 1, (short) 6).configs(configMap))).all().get();
        waitForTopicCreated(testTopicName);

        try {
            killBroker(0);
            if (isKRaftTest()) {
                ensureConsistentKRaftMetadata();
            } else {
                kafka.utils.TestUtils.waitUntilTrue(
                    () -> aliveBrokers().forall(b -> b.metadataCache().getPartitionInfo(testTopicName, 0).get().isr().size() == 5),
                    () -> String.format("Timeout waiting for partition metadata propagating to brokers for %s topic", testTopicName),
                    org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L
                );
            }
            String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--under-min-isr-partitions"));
            String[] rows = output.split("\n");
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)));
        } finally {
            restartDeadBrokers(false);
        }
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnderReplicatedPartitionsWhenReassignmentIsInProgress(String quorum) throws ExecutionException, InterruptedException {
        Map<String, String> configMap = new HashMap<>();
        short replicationFactor = 1;
        int partitions = 1;
        TopicPartition tp = new TopicPartition(testTopicName, 0);

        adminClient.createTopics(
            Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor).configs(configMap))
        ).all().get();
        waitForTopicCreated(testTopicName);

        // Produce multiple batches.
        TestUtils.generateAndProduceMessages(brokers(), testTopicName, 10, -1);
        TestUtils.generateAndProduceMessages(brokers(), testTopicName, 10, -1);

        // Enable throttling. Note the broker config sets the replica max fetch bytes to `1` upon to minimize replication
        // throughput so the reassignment doesn't complete quickly.
        List<Integer> brokerIds = JavaConverters.seqAsJavaList(brokers()).stream()
            .map(broker -> broker.config().brokerId()).collect(Collectors.toList());

        ToolsTestUtils.setReplicationThrottleForPartitions(adminClient, brokerIds, Collections.singleton(tp), 1);

        TopicDescription testTopicDesc = adminClient.describeTopics(Collections.singleton(testTopicName)).allTopicNames().get().get(testTopicName);
        TopicPartitionInfo firstPartition = testTopicDesc.partitions().get(0);

        List<Integer> replicasOfFirstPartition = firstPartition.replicas().stream().map(Node::id).collect(Collectors.toList());
        List<Integer> replicasDiff = new ArrayList<>(brokerIds);
        replicasDiff.removeAll(replicasOfFirstPartition);
        Integer targetReplica = replicasDiff.get(0);

        adminClient.alterPartitionReassignments(Collections.singletonMap(tp,
            Optional.of(new NewPartitionReassignment(Collections.singletonList(targetReplica))))).all().get();

        // let's wait until the LAIR is propagated
        kafka.utils.TestUtils.waitUntilTrue(
            () -> {
                try {
                    return !adminClient.listPartitionReassignments(Collections.singleton(tp)).reassignments().get()
                        .get(tp).addingReplicas().isEmpty();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            },
            () -> "Reassignment didn't add the second node",
            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);

        // describe the topic and test if it's under-replicated
        String simpleDescribeOutput = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName));
        String[] simpleDescribeOutputRows = simpleDescribeOutput.split("\n");
        assertTrue(simpleDescribeOutputRows[0].startsWith(String.format("Topic: %s", testTopicName)));
        assertEquals(2, simpleDescribeOutputRows.length);

        String underReplicatedOutput = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--under-replicated-partitions"));
        assertEquals("", underReplicatedOutput,
            String.format("--under-replicated-partitions shouldn't return anything: '%s'", underReplicatedOutput));

        // Verify reassignment is still ongoing.
        PartitionReassignment reassignments = adminClient.listPartitionReassignments(Collections.singleton(tp)).reassignments().get().get(tp);
        assertFalse(reassignments.addingReplicas().isEmpty());

        ToolsTestUtils.removeReplicationThrottleForPartitions(adminClient, brokerIds, Collections.singleton(tp));
        TestUtils.waitForAllReassignmentsToComplete(adminClient, 100L);
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeAtMinIsrPartitions(String quorum) throws ExecutionException, InterruptedException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");

        adminClient.createTopics(
            Collections.singletonList(new NewTopic(testTopicName, 1, (short) 6).configs(configMap))).all().get();
        waitForTopicCreated(testTopicName);

        try {
            killBroker(0);
            killBroker(1);

            if (isKRaftTest()) {
                ensureConsistentKRaftMetadata();
            } else {
                kafka.utils.TestUtils.waitUntilTrue(
                    () -> aliveBrokers().forall(broker -> broker.metadataCache().getPartitionInfo(testTopicName, 0).get().isr().size() == 4),
                    () -> String.format("Timeout waiting for partition metadata propagating to brokers for %s topic", testTopicName),
                    org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L
                );
            }

            String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--at-min-isr-partitions"));
            String[] rows = output.split("\n");
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)));
            assertEquals(1, rows.length);
        } finally {
            restartDeadBrokers(false);
        }
    }

    /**
     * Test describe --under-min-isr-partitions option with four topics:
     *   (1) topic with partition under the configured min ISR count
     *   (2) topic with under-replicated partition (but not under min ISR count)
     *   (3) topic with offline partition
     *   (4) topic with fully replicated partition
     *
     * Output should only display the (1) topic with partition under min ISR count and (3) topic with offline partition
     */
    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnderMinIsrPartitionsMixed(String quorum) throws ExecutionException, InterruptedException {
        String underMinIsrTopic = "under-min-isr-topic";
        String notUnderMinIsrTopic = "not-under-min-isr-topic";
        String offlineTopic = "offline-topic";
        String fullyReplicatedTopic = "fully-replicated-topic";

        Map<String, String> configMap = new HashMap<>();
        configMap.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6");

        adminClient.createTopics(
            java.util.Arrays.asList(
                new NewTopic(underMinIsrTopic, 1, (short) 6).configs(configMap),
                new NewTopic(notUnderMinIsrTopic, 1, (short) 6),
                new NewTopic(offlineTopic, Collections.singletonMap(0, Collections.singletonList(0))),
                new NewTopic(fullyReplicatedTopic, Collections.singletonMap(0, java.util.Arrays.asList(1, 2, 3))))
        ).all().get();

        waitForTopicCreated(underMinIsrTopic);
        waitForTopicCreated(notUnderMinIsrTopic);
        waitForTopicCreated(offlineTopic);
        waitForTopicCreated(fullyReplicatedTopic);

        try {
            killBroker(0);
            if (isKRaftTest()) {
                ensureConsistentKRaftMetadata();
            } else {
                kafka.utils.TestUtils.waitUntilTrue(
                    () -> aliveBrokers().forall(broker ->
                        broker.metadataCache().getPartitionInfo(underMinIsrTopic, 0).get().isr().size() < 6 &&
                            broker.metadataCache().getPartitionInfo(offlineTopic, 0).get().leader() == MetadataResponse.NO_LEADER_ID),
                    () -> "Timeout waiting for partition metadata propagating to brokers for underMinIsrTopic topic",
                    org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);
            }
            String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--under-min-isr-partitions"));
            String[] rows = output.split("\n");
            assertTrue(rows[0].startsWith(String.format("Topic: %s", underMinIsrTopic)));
            assertTrue(rows[1].startsWith(String.format("\tTopic: %s", offlineTopic)));
            assertEquals(2, rows.length);
        } finally {
            restartDeadBrokers(false);
        }
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeReportOverriddenConfigs(String quorum) throws Exception {
        String config = "file.delete.delay.ms=1000";
        createAndWaitTopic(buildTopicCommandOptionsWithBootstrap("--create", "--partitions", "2",
            "--replication-factor", "2", "--topic", testTopicName, "--config", config));
        String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe"));
        assertTrue(output.contains(config), String.format("Describe output should have contained %s", config));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeAndListTopicsWithoutInternalTopics(String quorum) throws Exception {
        createAndWaitTopic(
            buildTopicCommandOptionsWithBootstrap("--create", "--partitions", "1", "--replication-factor", "1", "--topic", testTopicName));
        // create a internal topic
        createAndWaitTopic(
            buildTopicCommandOptionsWithBootstrap("--create", "--partitions", "1", "--replication-factor", "1", "--topic", Topic.GROUP_METADATA_TOPIC_NAME));

        // test describe
        String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--describe", "--exclude-internal"));
        assertTrue(output.contains(testTopicName),
            String.format("Output should have contained %s", testTopicName));
        assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME));

        // test list
        output = captureListTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--list", "--exclude-internal"));
        assertTrue(output.contains(testTopicName));
        assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME));
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeDoesNotFailWhenListingReassignmentIsUnauthorized(String quorum) throws Exception {
        adminClient = spy(adminClient);
        topicService.close(); // need to be closed before initializing a new one with the spy version of adminclient, otherwise will have the extra adminclient(s) not closed.
        ListPartitionReassignmentsResult result = AdminClientTestUtils.listPartitionReassignmentsResult(
                new ClusterAuthorizationException("Unauthorized"));

        doReturn(result).when(adminClient).listPartitionReassignments(
                Collections.singleton(new TopicPartition(testTopicName, 0))
        );

        topicService = new TopicCommand.TopicService(adminClient);

        adminClient.createTopics(
                Collections.singletonList(new NewTopic(testTopicName, 1, (short) 1))
        ).all().get();
        waitForTopicCreated(testTopicName);

        String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName));
        String[] rows = output.split("\n");
        assertEquals(2, rows.length, "Unexpected output: " + output);
        assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)), "Unexpected output: " + rows[0]);
    }

    @ParameterizedTest(name = ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithTopicNameCollision(String quorum) throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            Collections.singletonList(new NewTopic("foo_bar", 1, (short) 6))).all().get();
        waitForTopicCreated("foo_bar");

        assertThrows(InvalidTopicException.class,
            () -> topicService.createTopic(buildTopicCommandOptionsWithBootstrap("--create", "--topic", "foo.bar")));
    }

    private void checkReplicaDistribution(Map<Integer, List<Integer>> assignment,
                                         Map<Integer, String> brokerRackMapping,
                                         Integer numBrokers,
                                         Integer numPartitions,
                                         Integer replicationFactor,
                                         Boolean verifyRackAware,
                                         Boolean verifyLeaderDistribution,
                                         Boolean verifyReplicasDistribution) {
        // always verify that no broker will be assigned for more than one replica
        assignment.entrySet().stream()
            .forEach(entry -> assertEquals(new HashSet<>(entry.getValue()).size(), entry.getValue().size(),
                "More than one replica is assigned to same broker for the same partition"));

        ReplicaDistributions distribution = getReplicaDistribution(assignment, brokerRackMapping);

        if (verifyRackAware) {
            Map<Integer, List<String>> partitionRackMap = distribution.partitionRacks;

            List<Integer> partitionRackMapValueSize = partitionRackMap.values().stream()
                .map(value -> (int) value.stream().distinct().count())
                .collect(Collectors.toList());

            List<Integer> expected = Collections.nCopies(numPartitions, replicationFactor);
            assertEquals(expected, partitionRackMapValueSize, "More than one replica of the same partition is assigned to the same rack");
        }

        if (verifyLeaderDistribution) {
            Map<Integer, Integer> leaderCount = distribution.brokerLeaderCount;
            int leaderCountPerBroker = numPartitions / numBrokers;
            List<Integer> expected = Collections.nCopies(numBrokers, leaderCountPerBroker);
            assertEquals(expected, new ArrayList<>(leaderCount.values()), "Preferred leader count is not even for brokers");
        }

        if (verifyReplicasDistribution) {
            Map<Integer, Integer> replicasCount = distribution.brokerReplicasCount;
            int numReplicasPerBroker = numPartitions * replicationFactor / numBrokers;
            List<Integer> expected = Collections.nCopies(numBrokers, numReplicasPerBroker);
            assertEquals(expected, new ArrayList<>(replicasCount.values()), "Replica count is not even for broker");
        }
    }

    private String captureDescribeTopicStandardOut(TopicCommand.TopicCommandOptions opts) {
        Runnable runnable = () -> {
            try {
                topicService.describeTopic(opts);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return ToolsTestUtils.captureStandardOut(runnable);
    }

    private String captureListTopicStandardOut(TopicCommand.TopicCommandOptions opts) {
        Runnable runnable = () -> {
            try {
                topicService.listTopics(opts);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return ToolsTestUtils.captureStandardOut(runnable);
    }

    private static ReplicaDistributions getReplicaDistribution(Map<Integer, List<Integer>> assignment, Map<Integer, String> brokerRackMapping) {
        Map<Integer, Integer> leaderCount = new HashMap<>();
        Map<Integer, Integer> partitionCount = new HashMap<>();
        Map<Integer, List<String>>  partitionRackMap = new HashMap<>();

        assignment.entrySet().stream().forEach(entry -> {
            Integer partitionId = entry.getKey();
            List<Integer> replicaList = entry.getValue();
            Integer leader = replicaList.get(0);
            leaderCount.put(leader, leaderCount.getOrDefault(leader, 0) + 1);
            replicaList.stream().forEach(brokerId -> {
                partitionCount.put(brokerId, partitionCount.getOrDefault(brokerId, 0) + 1);
                String rack;
                if (brokerRackMapping.containsKey(brokerId)) {
                    rack = brokerRackMapping.get(brokerId);
                    List<String> partitionRackValues = Stream.of(Collections.singletonList(rack), partitionRackMap.getOrDefault(partitionId, Collections.emptyList()))
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
                    partitionRackMap.put(partitionId, partitionRackValues);
                } else {
                    System.err.println(String.format("No mapping found for %s in `brokerRackMapping`", brokerId));
                }
            });
        });
        return new ReplicaDistributions(partitionRackMap, leaderCount, partitionCount);
    }

    private static class ReplicaDistributions {
        private final Map<Integer, List<String>>  partitionRacks;
        private final Map<Integer, Integer> brokerLeaderCount;
        private final Map<Integer, Integer> brokerReplicasCount;

        public ReplicaDistributions(Map<Integer, List<String>> partitionRacks,
                                    Map<Integer, Integer> brokerLeaderCount,
                                    Map<Integer, Integer> brokerReplicasCount) {
            this.partitionRacks = partitionRacks;
            this.brokerLeaderCount = brokerLeaderCount;
            this.brokerReplicasCount = brokerReplicasCount;
        }
    }

}
