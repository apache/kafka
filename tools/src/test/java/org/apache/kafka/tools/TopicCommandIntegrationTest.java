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
import kafka.server.ControllerServer;
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
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
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
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG;
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
    private final short defaultReplicationFactor = 1;
    private final int defaultNumPartitions = 1;
    private TopicCommand.TopicService topicService;
    private Admin adminClient;
    private String bootstrapServer;
    private String testTopicName;
    private Buffer<KafkaBroker> scalaBrokers;
    private Seq<ControllerServer> scalaControllers;

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
            .createBrokerProperties(6, zkConnectOrNull(), rackInfo, defaultNumPartitions, defaultReplicationFactor);

        List<KafkaConfig> configs = new ArrayList<>();
        for (Properties props : brokerConfigs) {
            props.put(REPLICA_FETCH_MAX_BYTES_CONFIG, "1");
            configs.add(KafkaConfig.fromProps(props));
        }
        return JavaConverters.asScalaBuffer(configs).toSeq();
    }

    private TopicCommand.TopicCommandOptions buildTopicCommandOptionsWithBootstrap(String... opts) {
        String[] finalOptions = Stream.concat(Arrays.stream(opts),
                Stream.of("--bootstrap-server", bootstrapServer)
        ).toArray(String[]::new);
        return new TopicCommand.TopicCommandOptions(finalOptions);
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
        testTopicName = String.format("%s-%s", info.getTestMethod().get().getName(), org.apache.kafka.test.TestUtils.randomString(10));
        scalaBrokers = brokers();
        scalaControllers = controllerServers();
    }

    @AfterEach
    public void close() throws Exception {
        if (topicService != null)
            topicService.close();
        if (adminClient != null)
            adminClient.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreate(String quorum) throws Exception {
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, 2, 1,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        assertTrue(adminClient.listTopics().names().get().contains(testTopicName),
                "Admin client didn't see the created topic. It saw: " + adminClient.listTopics().names().get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithDefaults(String quorum) throws Exception {
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        List<TopicPartitionInfo> partitions = adminClient
            .describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames()
            .get()
            .get(testTopicName)
            .partitions();
        assertEquals(defaultNumPartitions, partitions.size(), "Unequal partition size: " + partitions.size());
        assertEquals(defaultReplicationFactor, (short) partitions.get(0).replicas().size(), "Unequal replication factor: " + partitions.get(0).replicas().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithDefaultReplication(String quorum) throws Exception {
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, 2, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        List<TopicPartitionInfo>  partitions = adminClient
            .describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames()
            .get()
            .get(testTopicName)
            .partitions();
        assertEquals(2, partitions.size(), "Unequal partition size: " + partitions.size());
        assertEquals(defaultReplicationFactor, (short) partitions.get(0).replicas().size(), "Unequal replication factor: " + partitions.get(0).replicas().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithDefaultPartitions(String quorum) throws Exception {
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, 2,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        List<TopicPartitionInfo> partitions = adminClient
            .describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames()
            .get()
            .get(testTopicName)
            .partitions();

        assertEquals(defaultNumPartitions, partitions.size(), "Unequal partition size: " + partitions.size());
        assertEquals(2, (short) partitions.get(0).replicas().size(), "Partitions not replicated: " + partitions.get(0).replicas().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithConfigs(String quorum) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Properties topicConfig = new Properties();
        topicConfig.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "1000");

        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, 2, 2,
                scala.collection.immutable.Map$.MODULE$.empty(), topicConfig
        );

        Config configs = adminClient.describeConfigs(Collections.singleton(configResource)).all().get().get(configResource);
        assertEquals(1000, Integer.valueOf(configs.get("delete.retention.ms").value()),
                "Config not set correctly: " + configs.get("delete.retention.ms").value());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWhenAlreadyExists(String quorum) {
        // create the topic
        TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap(
            "--create", "--partitions", Integer.toString(defaultNumPartitions), "--replication-factor", "1",
                "--topic", testTopicName);

        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        // try to re-create the topic
        assertThrows(TopicExistsException.class, () -> topicService.createTopic(createOpts),
                "Expected TopicExistsException to throw");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWhenAlreadyExistsWithIfNotExists(String quorum) throws Exception {
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TopicCommand.TopicCommandOptions createOpts =
                buildTopicCommandOptionsWithBootstrap("--create", "--topic", testTopicName, "--if-not-exists");
        topicService.createTopic(createOpts);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithReplicaAssignment(String quorum) throws Exception {
        scala.collection.mutable.HashMap<Object, Seq<Object>> replicaAssignmentMap = new scala.collection.mutable.HashMap<>();

        replicaAssignmentMap.put(0, JavaConverters.asScalaBufferConverter(Arrays.asList((Object) 5, (Object) 4)).asScala().toSeq());
        replicaAssignmentMap.put(1, JavaConverters.asScalaBufferConverter(Arrays.asList((Object) 3, (Object) 2)).asScala().toSeq());
        replicaAssignmentMap.put(2, JavaConverters.asScalaBufferConverter(Arrays.asList((Object) 1, (Object) 0)).asScala().toSeq());

        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions,
                defaultReplicationFactor, replicaAssignmentMap, new Properties()
        );

        List<TopicPartitionInfo> partitions = adminClient
            .describeTopics(Collections.singletonList(testTopicName))
            .allTopicNames()
            .get()
            .get(testTopicName)
            .partitions();
        
        assertEquals(3, partitions.size(),
                "Unequal partition size: " + partitions.size());
        assertEquals(Arrays.asList(5, 4), getPartitionReplicas(partitions, 0),
                "Unexpected replica assignment: " + getPartitionReplicas(partitions, 0));
        assertEquals(Arrays.asList(3, 2), getPartitionReplicas(partitions, 1),
                "Unexpected replica assignment: " + getPartitionReplicas(partitions, 1));
        assertEquals(Arrays.asList(1, 0), getPartitionReplicas(partitions, 2),
                "Unexpected replica assignment: " + getPartitionReplicas(partitions, 2));
    }

    private List<Integer> getPartitionReplicas(List<TopicPartitionInfo> partitions, int partitionNumber) {
        return partitions.get(partitionNumber).replicas().stream().map(Node::id).collect(Collectors.toList());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithInvalidReplicationFactor(String quorum) {
        TopicCommand.TopicCommandOptions opts = buildTopicCommandOptionsWithBootstrap("--create", "--partitions", "2", "--replication-factor", Integer.toString(Short.MAX_VALUE + 1),
            "--topic", testTopicName);
        assertThrows(IllegalArgumentException.class, () -> topicService.createTopic(opts), "Expected IllegalArgumentException to throw");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithNegativeReplicationFactor(String quorum) {
        TopicCommand.TopicCommandOptions opts = buildTopicCommandOptionsWithBootstrap("--create",
            "--partitions", "2", "--replication-factor", "-1", "--topic", testTopicName);
        assertThrows(IllegalArgumentException.class, () -> topicService.createTopic(opts), "Expected IllegalArgumentException to throw");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithNegativePartitionCount(String quorum) {
        TopicCommand.TopicCommandOptions opts = buildTopicCommandOptionsWithBootstrap("--create", "--partitions", "-1", "--replication-factor", "1", "--topic", testTopicName);
        assertThrows(IllegalArgumentException.class, () -> topicService.createTopic(opts), "Expected IllegalArgumentException to throw");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testInvalidTopicLevelConfig(String quorum) {
        TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap("--create",
            "--partitions", "1", "--replication-factor", "1", "--topic", testTopicName,
            "--config", "message.timestamp.type=boom");
        assertThrows(ConfigException.class, () -> topicService.createTopic(createOpts), "Expected ConfigException to throw");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testListTopics(String quorum) {
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );

        String output = captureListTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--list"));
        assertTrue(output.contains(testTopicName), "Expected topic name to be present in output: " + output);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testListTopicsWithIncludeList(String quorum) {
        String topic1 = "kafka.testTopic1";
        String topic2 = "kafka.testTopic2";
        String topic3 = "oooof.testTopic1";
        int partition = 2;
        short replicationFactor = 2;
        TestUtils.createTopicWithAdmin(adminClient, topic1, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, topic2, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, topic3, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );

        String output = captureListTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--list", "--topic", "kafka.*"));

        assertTrue(output.contains(topic1), "Expected topic name " + topic1 + " to be present in output: " + output);
        assertTrue(output.contains(topic2), "Expected topic name " + topic2 + " to be present in output: " + output);
        assertFalse(output.contains(topic3), "Do not expect topic name " + topic3 + " to be present in output: " + output);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testListTopicsWithExcludeInternal(String quorum) {
        String topic1 = "kafka.testTopic1";
        String hiddenConsumerTopic = Topic.GROUP_METADATA_TOPIC_NAME;
        int partition = 2;
        short replicationFactor = 2;
        TestUtils.createTopicWithAdmin(adminClient, topic1, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, hiddenConsumerTopic, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );

        String output = captureListTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--list", "--exclude-internal"));

        assertTrue(output.contains(topic1), "Expected topic name " + topic1 + " to be present in output: " + output);
        assertFalse(output.contains(hiddenConsumerTopic), "Do not expect topic name " + hiddenConsumerTopic + " to be present in output: " + output);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterPartitionCount(String quorum) throws ExecutionException, InterruptedException {
        int partition = 2;
        short replicationFactor = 2;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter", "--topic", testTopicName, "--partitions", "3"));

        TestUtils.waitForAllReassignmentsToComplete(adminClient, 100L);
        kafka.utils.TestUtils.waitUntilTrue(
            () -> brokers().forall(b -> b.metadataCache().getTopicPartitions(testTopicName).size() == 3),
            () -> "Timeout waiting for new assignment propagating to broker", org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);
        TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues().get(testTopicName).get();
        assertEquals(3, topicDescription.partitions().size(), "Expected partition count to be 3. Got: " + topicDescription.partitions().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterAssignment(String quorum) throws ExecutionException, InterruptedException {
        int partition = 2;
        short replicationFactor = 2;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter",
            "--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "3"));

        TestUtils.waitForAllReassignmentsToComplete(adminClient, 100L);
        kafka.utils.TestUtils.waitUntilTrue(
            () -> brokers().forall(b -> b.metadataCache().getTopicPartitions(testTopicName).size() == 3),
            () -> "Timeout waiting for new assignment propagating to broker",
            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 100L);

        TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues().get(testTopicName).get();
        assertEquals(3, topicDescription.partitions().size(), "Expected partition count to be 3. Got: " + topicDescription.partitions().size());
        List<Integer> partitionReplicas = getPartitionReplicas(topicDescription.partitions(), 2);
        assertEquals(Arrays.asList(4, 2), partitionReplicas, "Expected to have replicas 4,2. Got: " + partitionReplicas);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterAssignmentWithMoreAssignmentThanPartitions(String quorum) {
        int partition = 2;
        short replicationFactor = 2;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        assertThrows(ExecutionException.class,
            () -> topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter",
                "--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2,3:2", "--partitions", "3")),
                "Expected to fail with ExecutionException");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterAssignmentWithMorePartitionsThanAssignment(String quorum) {
        int partition = 2;
        short replicationFactor = 2;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );

        assertThrows(ExecutionException.class,
            () -> topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter", "--topic", testTopicName,
                "--replica-assignment", "5:3,3:1,4:2", "--partitions", "6")),
                "Expected to fail with ExecutionException");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterWithInvalidPartitionCount(String quorum) {
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        assertThrows(ExecutionException.class,
            () -> topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter", "--partitions", "-1", "--topic", testTopicName)),
                "Expected to fail with ExecutionException");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterWhenTopicDoesntExist(String quorum) {
        // alter a topic that does not exist without --if-exists
        TopicCommand.TopicCommandOptions alterOpts = buildTopicCommandOptionsWithBootstrap("--alter", "--topic", testTopicName, "--partitions", "1");
        TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient);
        assertThrows(IllegalArgumentException.class, () -> topicService.alterTopic(alterOpts), "Expected to fail with IllegalArgumentException");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testAlterWhenTopicDoesntExistWithIfExists(String quorum) throws ExecutionException, InterruptedException {
        topicService.alterTopic(buildTopicCommandOptionsWithBootstrap("--alter", "--topic", testTopicName, "--partitions", "1", "--if-exists"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateAlterTopicWithRackAware(String quorum) throws Exception {
        Map<Integer, String> rackInfo = new HashMap<>();
        rackInfo.put(0, "rack1");
        rackInfo.put(1, "rack2");
        rackInfo.put(2, "rack2");
        rackInfo.put(3, "rack1");
        rackInfo.put(4, "rack3");
        rackInfo.put(5, "rack3");

        int numPartitions = 18;
        int replicationFactor = 3;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, numPartitions, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );

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

        TestUtils.waitForAllReassignmentsToComplete(adminClient, 100L);
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

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testConfigPreservationAcrossPartitionAlteration(String quorum) throws Exception {
        String cleanUpPolicy = "compact";
        Properties topicConfig = new Properties();
        topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanUpPolicy);
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), topicConfig
        );

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Config props = adminClient.describeConfigs(Collections.singleton(configResource)).all().get().get(configResource);
        // val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, testTopicName)
        assertNotNull(props.get(TopicConfig.CLEANUP_POLICY_CONFIG), "Properties after creation don't contain " + cleanUpPolicy);
        assertEquals(cleanUpPolicy, props.get(TopicConfig.CLEANUP_POLICY_CONFIG).value(), "Properties after creation have incorrect value");

        // pre-create the topic config changes path to avoid a NoNodeException
        if (!isKRaftTest()) {
            zkClient().makeSurePersistentPathExists(kafka.zk.ConfigEntityChangeNotificationZNode.path());
        }

        // modify the topic to add new partitions
        int numPartitionsModified = 3;
        TopicCommand.TopicCommandOptions alterOpts = buildTopicCommandOptionsWithBootstrap("--alter",
            "--partitions", Integer.toString(numPartitionsModified), "--topic", testTopicName);
        topicService.alterTopic(alterOpts);

        TestUtils.waitForAllReassignmentsToComplete(adminClient, 100L);
        Config newProps = adminClient.describeConfigs(Collections.singleton(configResource)).all().get().get(configResource);
        assertNotNull(newProps.get(TopicConfig.CLEANUP_POLICY_CONFIG), "Updated properties do not contain " + TopicConfig.CLEANUP_POLICY_CONFIG);
        assertEquals(cleanUpPolicy, newProps.get(TopicConfig.CLEANUP_POLICY_CONFIG).value(), "Updated properties have incorrect value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testTopicDeletion(String quorum) throws Exception {
        // create the NormalTopic
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        // delete the NormalTopic
        TopicCommand.TopicCommandOptions deleteOpts = buildTopicCommandOptionsWithBootstrap("--delete", "--topic", testTopicName);

        if (!isKRaftTest()) {
            String deletePath = kafka.zk.DeleteTopicsTopicZNode.path(testTopicName);
            assertFalse(zkClient().pathExists(deletePath), "Delete path for topic shouldn't exist before deletion.");
        }
        topicService.deleteTopic(deleteOpts);
        TestUtils.verifyTopicDeletion(zkClientOrNull(), testTopicName, 1, brokers());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testTopicWithCollidingCharDeletionAndCreateAgain(String quorum) throws Exception {
        // create the topic with colliding chars
        String topicWithCollidingChar = "test.a";
        TestUtils.createTopicWithAdmin(adminClient, topicWithCollidingChar, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        // delete the topic
        TopicCommand.TopicCommandOptions deleteOpts = buildTopicCommandOptionsWithBootstrap("--delete", "--topic", topicWithCollidingChar);

        if (!isKRaftTest()) {
            String deletePath = kafka.zk.DeleteTopicsTopicZNode.path(topicWithCollidingChar);
            assertFalse(zkClient().pathExists(deletePath), "Delete path for topic shouldn't exist before deletion.");
        }
        topicService.deleteTopic(deleteOpts);
        TestUtils.verifyTopicDeletion(zkClientOrNull(), topicWithCollidingChar, 1, brokers());
        assertDoesNotThrow(() -> TestUtils.createTopicWithAdmin(adminClient, topicWithCollidingChar, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        ), "Should be able to create a topic with colliding chars after deletion.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteInternalTopic(String quorum) throws Exception {
        // create the offset topic
        TestUtils.createTopicWithAdmin(adminClient, Topic.GROUP_METADATA_TOPIC_NAME, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
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
        TestUtils.verifyTopicDeletion(zkClientOrNull(), Topic.GROUP_METADATA_TOPIC_NAME, defaultNumPartitions, brokers());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteWhenTopicDoesntExist(String quorum) {
        // delete a topic that does not exist
        TopicCommand.TopicCommandOptions deleteOpts = buildTopicCommandOptionsWithBootstrap("--delete", "--topic", testTopicName);
        assertThrows(IllegalArgumentException.class, () -> topicService.deleteTopic(deleteOpts),
                "Expected an exception when trying to delete a topic that does not exist.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDeleteWhenTopicDoesntExistWithIfExists(String quorum) throws ExecutionException, InterruptedException {
        topicService.deleteTopic(buildTopicCommandOptionsWithBootstrap("--delete", "--topic", testTopicName, "--if-exists"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribe(String quorum) {
        int partition = 2;
        short replicationFactor = 2;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partition, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName));
        String[] rows = output.split(System.lineSeparator());
        assertEquals(3, rows.length, "Expected 3 rows in output, got " + rows.length);
        assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)), "Row does not start with " + testTopicName + ". Row is: " + rows[0]);
    }

    @ParameterizedTest
    @ValueSource(strings = {"quorum=zk", "quorum=kraft"})
    public void testDescribeWithDescribeTopicPartitionsApi(String quorum) throws ExecutionException, InterruptedException {
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, 20, (short) 2,
            scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, "test-2", scalaBrokers, scalaControllers, 41, (short) 2,
            scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, "test-3", scalaBrokers, scalaControllers, 5, (short) 2,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, "test-4", scalaBrokers, scalaControllers, 5, (short) 2,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, "test-5", scalaBrokers, scalaControllers, 100, (short) 2,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );

        String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap(
            "--describe", "--partition-size-limit-per-response=20"));
        String[] rows = output.split("\n");
        assertEquals(176, rows.length, String.join("\n", rows));
        assertTrue(rows[2].contains("\tElr"), rows[2]);
        assertTrue(rows[2].contains("LastKnownElr"), rows[2]);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeWhenTopicDoesntExist(String quorum) {
        assertThrows(IllegalArgumentException.class,
            () -> topicService.describeTopic(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName)),
                "Expected an exception when trying to describe a topic that does not exist.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeWhenTopicDoesntExistWithIfExists(String quorum) throws ExecutionException, InterruptedException {
        topicService.describeTopic(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName, "--if-exists"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnavailablePartitions(String quorum) throws ExecutionException, InterruptedException {
        int partitions = 6;
        short replicationFactor = 1;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partitions, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
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
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)),
                    "Unexpected Topic " + rows[0] + " received. Expect " + String.format("Topic: %s", testTopicName));
            assertTrue(rows[0].contains("Leader: none\tReplicas: 0\tIsr:"),
                    "Rows did not contain 'Leader: none\tReplicas: 0\tIsr:'");
        } finally {
            restartDeadBrokers(false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnderReplicatedPartitions(String quorum) {
        int partitions = 1;
        short replicationFactor = 6;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partitions, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        try {
            killBroker(0);
            if (isKRaftTest()) {
                ensureConsistentKRaftMetadata();
            } else {
                TestUtils.waitForPartitionMetadata(aliveBrokers(), testTopicName, 0, org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS);
            }
            String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--under-replicated-partitions"));
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)), String.format("Unexpected output: %s", rows[0]));
        } finally {
            restartDeadBrokers(false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnderMinIsrPartitions(String quorum) {
        Properties topicConfig = new Properties();
        topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6");
        int partitions = 1;
        short replicationFactor = 6;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partitions, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), topicConfig
        );
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
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)),
                    "Unexpected topic: " + rows[0]);
        } finally {
            restartDeadBrokers(false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnderReplicatedPartitionsWhenReassignmentIsInProgress(String quorum) throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(testTopicName, 0);

        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );

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

        ensureConsistentKRaftMetadata();

        // describe the topic and test if it's under-replicated
        String simpleDescribeOutput = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName));
        String[] simpleDescribeOutputRows = simpleDescribeOutput.split(System.lineSeparator());
        assertTrue(simpleDescribeOutputRows[0].startsWith(String.format("Topic: %s", testTopicName)),
                "Unexpected describe output: " + simpleDescribeOutputRows[0]);
        assertEquals(2, simpleDescribeOutputRows.length,
                "Unexpected describe output length: " + simpleDescribeOutputRows.length);

        String underReplicatedOutput = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--under-replicated-partitions"));
        assertEquals("", underReplicatedOutput,
            String.format("--under-replicated-partitions shouldn't return anything: '%s'", underReplicatedOutput));

        int maxRetries = 20;
        long pause = 100L;
        long waitTimeMs = maxRetries * pause;
        AtomicReference<PartitionReassignment> reassignmentsRef = new AtomicReference<>();

        TestUtils.waitUntilTrue(() -> {
            try {
                PartitionReassignment tempReassignments = adminClient.listPartitionReassignments(Collections.singleton(tp)).reassignments().get().get(tp);
                reassignmentsRef.set(tempReassignments);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Error while fetching reassignments", e);
            }
            return reassignmentsRef.get() != null;
        }, () -> "Reassignments did not become non-null within the specified time", waitTimeMs, pause);

        assertFalse(reassignmentsRef.get().addingReplicas().isEmpty());

        ToolsTestUtils.removeReplicationThrottleForPartitions(adminClient, brokerIds, Collections.singleton(tp));
        TestUtils.waitForAllReassignmentsToComplete(adminClient, 100L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeAtMinIsrPartitions(String quorum) {
        Properties topicConfig = new Properties();
        topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");

        int partitions = 1;
        short replicationFactor = 6;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partitions, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), topicConfig
        );
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
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)),
                    "Unexpected output: " + rows[0]);
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
    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeUnderMinIsrPartitionsMixed(String quorum) {
        String underMinIsrTopic = "under-min-isr-topic";
        String notUnderMinIsrTopic = "not-under-min-isr-topic";
        String offlineTopic = "offline-topic";
        String fullyReplicatedTopic = "fully-replicated-topic";

        scala.collection.mutable.HashMap<Object, Seq<Object>> fullyReplicatedReplicaAssignmentMap = new scala.collection.mutable.HashMap<>();
        fullyReplicatedReplicaAssignmentMap.put(0, JavaConverters.asScalaBufferConverter(Arrays.asList((Object) 1, (Object) 2, (Object) 3)).asScala().toSeq());

        scala.collection.mutable.HashMap<Object, Seq<Object>> offlineReplicaAssignmentMap = new scala.collection.mutable.HashMap<>();
        offlineReplicaAssignmentMap.put(0, JavaConverters.asScalaBufferConverter(Arrays.asList((Object) 0)).asScala().toSeq());

        Properties topicConfig = new Properties();
        topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6");

        int partitions = 1;
        short replicationFactor = 6;
        int negativePartition = -1;
        short negativeReplicationFactor = -1;
        TestUtils.createTopicWithAdmin(adminClient, underMinIsrTopic, scalaBrokers, scalaControllers, partitions, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), topicConfig
        );
        TestUtils.createTopicWithAdmin(adminClient, notUnderMinIsrTopic, scalaBrokers, scalaControllers, partitions, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, offlineTopic, scalaBrokers, scalaControllers, negativePartition, negativeReplicationFactor,
                offlineReplicaAssignmentMap, new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, fullyReplicatedTopic, scalaBrokers, scalaControllers, negativePartition, negativeReplicationFactor,
                fullyReplicatedReplicaAssignmentMap, new Properties()
        );


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
            TestUtils.waitForAllReassignmentsToComplete(adminClient, 100L);
            String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--under-min-isr-partitions"));
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", underMinIsrTopic)),
                    "Unexpected output: " + rows[0]);
            assertTrue(rows[1].startsWith(String.format("\tTopic: %s", offlineTopic)),
                    "Unexpected output: " + rows[1]);
            assertEquals(2, rows.length);
        } finally {
            restartDeadBrokers(false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeReportOverriddenConfigs(String quorum) {
        String config = "file.delete.delay.ms=1000";
        Properties topicConfig = new Properties();
        topicConfig.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "1000");

        int partitions = 2;
        short replicationFactor = 2;
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, partitions, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), topicConfig
        );
        String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe"));
        assertTrue(output.contains(config), String.format("Describe output should have contained %s", config));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeAndListTopicsWithoutInternalTopics(String quorum) {
        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        TestUtils.createTopicWithAdmin(adminClient, Topic.GROUP_METADATA_TOPIC_NAME, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        // test describe
        String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--describe", "--exclude-internal"));
        assertTrue(output.contains(testTopicName),
            String.format("Output should have contained %s", testTopicName));
        assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME),
                "Output should not have contained " + Topic.GROUP_METADATA_TOPIC_NAME);

        // test list
        output = captureListTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--list", "--exclude-internal"));
        assertTrue(output.contains(testTopicName), String.format("Output should have contained %s", testTopicName));
        assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME),
                "Output should not have contained " + Topic.GROUP_METADATA_TOPIC_NAME);
    }

    @ParameterizedTest
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

        TestUtils.createTopicWithAdmin(adminClient, testTopicName, scalaBrokers, scalaControllers, defaultNumPartitions, defaultReplicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );

        String output = captureDescribeTopicStandardOut(buildTopicCommandOptionsWithBootstrap("--describe", "--topic", testTopicName));
        String[] rows = output.split(System.lineSeparator());
        assertEquals(2, rows.length, "Unexpected output: " + output);
        assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)), "Unexpected output: " + rows[0]);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testCreateWithTopicNameCollision(String quorum) {
        String topic = "foo_bar";
        int partitions = 1;
        short replicationFactor = 6;
        TestUtils.createTopicWithAdmin(adminClient, topic, scalaBrokers, scalaControllers, partitions, replicationFactor,
                scala.collection.immutable.Map$.MODULE$.empty(), new Properties()
        );
        assertThrows(TopicExistsException.class,
            () -> topicService.createTopic(buildTopicCommandOptionsWithBootstrap("--create", "--topic", topic)));
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
                    System.err.printf("No mapping found for %s in `brokerRackMapping`%n", brokerId);
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
