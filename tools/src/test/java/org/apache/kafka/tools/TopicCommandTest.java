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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.server.config.ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(ClusterTestExtensions.class)
public class TopicCommandTest {
    private final short defaultReplicationFactor = 1;
    private final int defaultNumPartitions = 1;

    private static final int CLUSTER_WAIT_MS = 60000;

    private final String bootstrapServer = "localhost:9092";
    private final String topicName = "topicName";

    @Test
    public void testIsNotUnderReplicatedWhenAdding() {
        List<Integer> replicaIds = Arrays.asList(1, 2);
        List<Node> replicas = new ArrayList<>();
        for (int id : replicaIds) {
            replicas.add(new Node(id, "localhost", 9090 + id));
        }

        TopicCommand.PartitionDescription partitionDescription = new TopicCommand.PartitionDescription("test-topic",
                new TopicPartitionInfo(0, new Node(1, "localhost", 9091), replicas,
                        Collections.singletonList(new Node(1, "localhost", 9091))),
                null, false,
                new PartitionReassignment(replicaIds, Arrays.asList(2), Collections.emptyList())
        );

        assertFalse(partitionDescription.isUnderReplicated());
    }

    @Test
    public void testAlterWithUnspecifiedPartitionCount() {
        String[] options = new String[] {" --bootstrap-server", bootstrapServer, "--alter", "--topic", topicName};
        assertInitializeInvalidOptionsExitCode(1, options);
    }

    @Test
    public void testConfigOptWithBootstrapServers() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--alter", "--topic", topicName,
                "--partitions", "3", "--config", "cleanup.policy=compact"});
        TopicCommand.TopicCommandOptions opts =
            new TopicCommand.TopicCommandOptions(
                new String[] {"--bootstrap-server", bootstrapServer, "--create", "--topic", topicName, "--partitions", "3",
                    "--replication-factor", "3", "--config", "cleanup.policy=compact"});
        assertTrue(opts.hasCreateOption());
        assertEquals(bootstrapServer, opts.bootstrapServer().get());
        assertEquals("cleanup.policy=compact", opts.topicConfig().get().get(0));
    }

    @Test
    public void testCreateWithPartitionCountWithoutReplicationFactorShouldSucceed() {
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer,
                "--create",
                "--partitions", "2",
                "--topic", topicName});
        assertTrue(opts.hasCreateOption());
        assertEquals(topicName, opts.topic().get());
        assertEquals(2, opts.partitions().get());
    }

    @Test
    public void testCreateWithReplicationFactorWithoutPartitionCountShouldSucceed() {
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer,
                "--create",
                "--replication-factor", "3",
                "--topic", topicName});
        assertTrue(opts.hasCreateOption());
        assertEquals(topicName, opts.topic().get());
        assertEquals(3, opts.replicationFactor().get());
    }

    @Test
    public void testCreateWithAssignmentAndPartitionCount() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[]{"--bootstrap-server", bootstrapServer,
                "--create",
                "--replica-assignment", "3:0,5:1",
                "--partitions", "2",
                "--topic", topicName});
    }

    @Test
    public void testCreateWithAssignmentAndReplicationFactor() {
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer,
                "--create",
                "--replica-assignment", "3:0,5:1",
                "--replication-factor", "2",
                "--topic", topicName});
    }

    @Test
    public void testCreateWithoutPartitionCountAndReplicationFactorShouldSucceed() {
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer,
                "--create",
                "--topic", topicName});
        assertTrue(opts.hasCreateOption());
        assertEquals(topicName, opts.topic().get());
        assertFalse(opts.partitions().isPresent());
    }

    @Test
    public void testDescribeShouldSucceed() {
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer,
                "--describe",
                "--topic", topicName});
        assertTrue(opts.hasDescribeOption());
        assertEquals(topicName, opts.topic().get());
    }

    @Test
    public void testDescribeWithDescribeTopicsApiShouldSucceed() {
        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(
            new String[] {"--bootstrap-server", bootstrapServer,
                "--describe",
                "--topic", topicName});
        assertTrue(opts.hasDescribeOption());
        assertEquals(topicName, opts.topic().get());
    }


    @Test
    public void testParseAssignmentDuplicateEntries() {
        assertThrows(AdminCommandFailedException.class, () -> TopicCommand.parseReplicaAssignment("5:5"));
    }

    @Test
    public void testParseAssignmentPartitionsOfDifferentSize() {
        assertThrows(AdminOperationException.class, () -> TopicCommand.parseReplicaAssignment("5:4:3,2:1"));
    }

    @Test
    public void testParseAssignment() {
        Map<Integer, List<Integer>> actualAssignment = TopicCommand.parseReplicaAssignment("5:4,3:2,1:0");
        Map<Integer, List<Integer>>  expectedAssignment = new HashMap<>();
        expectedAssignment.put(0,  Arrays.asList(5, 4));
        expectedAssignment.put(1, Arrays.asList(3, 2));
        expectedAssignment.put(2, Arrays.asList(1, 0));
        assertEquals(expectedAssignment, actualAssignment);
    }

    @Test
    public void testCreateTopicDoesNotRetryThrottlingQuotaExceededException() {
        Admin adminClient = mock(Admin.class);
        TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient);

        CreateTopicsResult result = AdminClientTestUtils.createTopicsResult(topicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception());
        when(adminClient.createTopics(any(), any())).thenReturn(result);

        assertThrows(ThrottlingQuotaExceededException.class,
            () -> topicService.createTopic(new TopicCommand.TopicCommandOptions(new String[]{
                "--bootstrap-server", bootstrapServer,
                "--create", "--topic", topicName
            })));

        NewTopic expectedNewTopic = new NewTopic(topicName, Optional.empty(), Optional.empty())
                .configs(Collections.emptyMap());

        verify(adminClient, times(1)).createTopics(
                eq(new HashSet<>(Arrays.asList(expectedNewTopic))),
                argThat(exception -> !exception.shouldRetryOnQuotaViolation())
        );
    }

    @Test
    public void testDeleteTopicDoesNotRetryThrottlingQuotaExceededException() {
        Admin adminClient = mock(Admin.class);
        TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient);

        ListTopicsResult listResult = AdminClientTestUtils.listTopicsResult(topicName);
        when(adminClient.listTopics(any())).thenReturn(listResult);

        DeleteTopicsResult result = AdminClientTestUtils.deleteTopicsResult(topicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception());
        when(adminClient.deleteTopics(anyCollection(), any())).thenReturn(result);

        ExecutionException exception = assertThrows(ExecutionException.class,
            () -> topicService.deleteTopic(new TopicCommand.TopicCommandOptions(new String[]{
                "--bootstrap-server", bootstrapServer,
                "--delete", "--topic", topicName
            })));

        assertInstanceOf(ThrottlingQuotaExceededException.class, exception.getCause());

        verify(adminClient).deleteTopics(
                argThat((Collection<String> topics) -> topics.equals(Arrays.asList(topicName))),
                argThat((DeleteTopicsOptions options) -> !options.shouldRetryOnQuotaViolation()));
    }

    @Test
    public void testCreatePartitionsDoesNotRetryThrottlingQuotaExceededException() {
        Admin adminClient = mock(Admin.class);
        TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient);

        ListTopicsResult listResult = AdminClientTestUtils.listTopicsResult(topicName);
        when(adminClient.listTopics(any())).thenReturn(listResult);

        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, new Node(0, "", 0),
                Collections.emptyList(), Collections.emptyList());
        DescribeTopicsResult describeResult = AdminClientTestUtils.describeTopicsResult(topicName,
                new TopicDescription(topicName, false, Collections.singletonList(topicPartitionInfo)));
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeResult);

        CreatePartitionsResult result = AdminClientTestUtils.createPartitionsResult(topicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception());
        when(adminClient.createPartitions(any(), any())).thenReturn(result);

        Exception exception = assertThrows(ExecutionException.class,
            () -> topicService.alterTopic(new TopicCommand.TopicCommandOptions(new String[]{
                "--alter", "--topic", topicName, "--partitions", "3",
                "--bootstrap-server", bootstrapServer
            })));
        assertInstanceOf(ThrottlingQuotaExceededException.class, exception.getCause());

        verify(adminClient, times(1)).createPartitions(
                argThat(newPartitions -> newPartitions.get(topicName).totalCount() == 3),
                argThat(createPartitionOption -> !createPartitionOption.shouldRetryOnQuotaViolation()));
    }

    public void assertInitializeInvalidOptionsExitCode(int expected, String[] options) {
        Exit.setExitProcedure((exitCode, message) -> {
            assertEquals(expected, exitCode);
            throw new RuntimeException();
        });
        try {
            assertThrows(RuntimeException.class, () -> new TopicCommand.TopicCommandOptions(options));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    private TopicCommand.TopicCommandOptions buildTopicCommandOptionsWithBootstrap(ClusterInstance clusterInstance, String... opts) {
        String bootstrapServer = clusterInstance.bootstrapServers();
        String[] finalOptions = Stream.concat(Arrays.stream(opts),
                Stream.of("--bootstrap-server", bootstrapServer)
        ).toArray(String[]::new);
        return new TopicCommand.TopicCommandOptions(finalOptions);
    }

    static List<ClusterConfig> generate() {
        Map<String, String> serverProp = new HashMap<>();
        serverProp.put(REPLICA_FETCH_MAX_BYTES_CONFIG, "1"); // if config name error, no exception throw
        serverProp.put("log.initial.task.delay.ms", "100");
        serverProp.put("log.segment.delete.delay.ms", "1000");

        Map<Integer, Map<String, String>> rackInfo = new HashMap<>();
        Map<String, String> infoPerBroker1 = new HashMap<>();
        infoPerBroker1.put("broker.rack", "rack1");
        Map<String, String> infoPerBroker2 = new HashMap<>();
        infoPerBroker2.put("broker.rack", "rack2");
        Map<String, String> infoPerBroker3 = new HashMap<>();
        infoPerBroker3.put("broker.rack", "rack2");
        Map<String, String> infoPerBroker4 = new HashMap<>();
        infoPerBroker4.put("broker.rack", "rack1");
        Map<String, String> infoPerBroker5 = new HashMap<>();
        infoPerBroker5.put("broker.rack", "rack3");
        Map<String, String> infoPerBroker6 = new HashMap<>();
        infoPerBroker6.put("broker.rack", "rack3");

        rackInfo.put(0, infoPerBroker1);
        rackInfo.put(1, infoPerBroker2);
        rackInfo.put(2, infoPerBroker3);
        rackInfo.put(3, infoPerBroker4);
        rackInfo.put(4, infoPerBroker5);
        rackInfo.put(5, infoPerBroker6);

        return Collections.singletonList(ClusterConfig.defaultBuilder()
                .setBrokers(6)
                .setServerProperties(serverProp)
                .setPerServerProperties(rackInfo)
                .setTypes(Stream.of(Type.KRAFT).collect(Collectors.toSet()))
                .build()
        );
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
        }
    )
    public void testCreate(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin()) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));

            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);
            Assertions.assertTrue(adminClient.listTopics().names().get().contains(testTopicName),
                    "Admin client didn't see the created topic. It saw: " + adminClient.listTopics().names().get());

            adminClient.deleteTopics(Collections.singletonList(testTopicName));
            clusterInstance.waitForTopic(testTopicName, 0);
            Assertions.assertTrue(adminClient.listTopics().names().get().isEmpty(),
                    "Admin client see the created topic. It saw: " + adminClient.listTopics().names().get());
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
        }
    )
    public void testCreateWithDefaults(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin()) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));

            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);
            Assertions.assertTrue(adminClient.listTopics().names().get().contains(testTopicName),
                    "Admin client didn't see the created topic. It saw: " + adminClient.listTopics().names().get());

            List<TopicPartitionInfo> partitions = adminClient
                    .describeTopics(Collections.singletonList(testTopicName))
                    .allTopicNames()
                    .get()
                    .get(testTopicName)
                    .partitions();
            Assertions.assertEquals(defaultNumPartitions, partitions.size(), "Unequal partition size: " + partitions.size());
            Assertions.assertEquals(defaultReplicationFactor, (short) partitions.get(0).replicas().size(), "Unequal replication factor: " + partitions.get(0).replicas().size());

            adminClient.deleteTopics(Collections.singletonList(testTopicName));
            clusterInstance.waitForTopic(testTopicName, 0);
            Assertions.assertTrue(adminClient.listTopics().names().get().isEmpty(),
                    "Admin client see the created topic. It saw: " + adminClient.listTopics().names().get());
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
        }
    )
    public void testCreateWithDefaultReplication(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin()) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, 2, defaultReplicationFactor)));
            clusterInstance.waitForTopic(testTopicName, 2);
            List<TopicPartitionInfo>  partitions = adminClient
                    .describeTopics(Collections.singletonList(testTopicName))
                    .allTopicNames()
                    .get()
                    .get(testTopicName)
                    .partitions();
            assertEquals(2, partitions.size(), "Unequal partition size: " + partitions.size());
            assertEquals(defaultReplicationFactor, (short) partitions.get(0).replicas().size(), "Unequal replication factor: " + partitions.get(0).replicas().size());
        }
    }

    @ClusterTest(brokers = 3)
    public void testCreateWithDefaultPartitions(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin()) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, (short) 2)));
            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);
            List<TopicPartitionInfo> partitions = adminClient
                    .describeTopics(Collections.singletonList(testTopicName))
                    .allTopicNames()
                    .get()
                    .get(testTopicName)
                    .partitions();

            assertEquals(defaultNumPartitions, partitions.size(), "Unequal partition size: " + partitions.size());
            assertEquals(2, (short) partitions.get(0).replicas().size(), "Partitions not replicated: " + partitions.get(0).replicas().size());
        }
    }

    @ClusterTest(brokers = 3)
    public void testCreateWithConfigs(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin()) {
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "1000");

            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, 2, (short) 2).configs(topicConfig)));
            clusterInstance.waitForTopic(testTopicName, 2);


            Config configs = adminClient.describeConfigs(Collections.singleton(configResource)).all().get().get(configResource);
            assertEquals(1000, Integer.valueOf(configs.get("delete.retention.ms").value()),
                    "Config not set correctly: " + configs.get("delete.retention.ms").value());
        }
    }

    @ClusterTest(brokers = 3)
    public void testCreateWhenAlreadyExists(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap(
                    clusterInstance, "--create", "--partitions", Integer.toString(defaultNumPartitions), "--replication-factor", "1",
                    "--topic", testTopicName);

            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);

            // try to re-create the topic
            assertThrows(TopicExistsException.class, () -> topicService.createTopic(createOpts),
                    "Expected TopicExistsException to throw");
        }
    }

    @ClusterTest(brokers = 3)
    public void testCreateWhenAlreadyExistsWithIfNotExists(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);

            TopicCommand.TopicCommandOptions createOpts =
                    buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--topic", testTopicName, "--if-not-exists");
            topicService.createTopic(createOpts);
        }
    }

    private List<Integer> getPartitionReplicas(List<TopicPartitionInfo> partitions, int partitionNumber) {
        return partitions.get(partitionNumber).replicas().stream().map(Node::id).collect(Collectors.toList());
    }

    @ClusterTemplate("generate")
    public void testCreateWithReplicaAssignment(ClusterInstance clusterInstance) throws Exception {
        Map<Integer, List<Integer>> replicaAssignmentMap = new HashMap<>();
        try (Admin adminClient = clusterInstance.admin()) {
            String testTopicName = TestUtils.randomString(10);

            replicaAssignmentMap.put(0, Arrays.asList(5, 4));
            replicaAssignmentMap.put(1, Arrays.asList(3, 2));
            replicaAssignmentMap.put(2, Arrays.asList(1, 0));

            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, replicaAssignmentMap)));
            clusterInstance.waitForTopic(testTopicName, 3);

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
    }

    @ClusterTest(brokers = 3)
    public void testCreateWithInvalidReplicationFactor(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {

            TopicCommand.TopicCommandOptions opts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--partitions", "2", "--replication-factor", Integer.toString(Short.MAX_VALUE + 1),
                    "--topic", testTopicName);
            assertThrows(IllegalArgumentException.class, () -> topicService.createTopic(opts), "Expected IllegalArgumentException to throw");
        }
    }

    @ClusterTest
    public void testCreateWithNegativeReplicationFactor(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            TopicCommand.TopicCommandOptions opts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create",
                    "--partitions", "2", "--replication-factor", "-1", "--topic", testTopicName);
            assertThrows(IllegalArgumentException.class, () -> topicService.createTopic(opts), "Expected IllegalArgumentException to throw");
        }
    }

    @ClusterTest
    public void testCreateWithNegativePartitionCount(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            TopicCommand.TopicCommandOptions opts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--partitions", "-1", "--replication-factor", "1", "--topic", testTopicName);
            assertThrows(IllegalArgumentException.class, () -> topicService.createTopic(opts), "Expected IllegalArgumentException to throw");
        }
    }

    @ClusterTest
    public void testInvalidTopicLevelConfig(ClusterInstance clusterInstance) {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin()) {
            TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient);

            TopicCommand.TopicCommandOptions createOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create",
                    "--partitions", "1", "--replication-factor", "1", "--topic", testTopicName,
                    "--config", "message.timestamp.type=boom");
            assertThrows(ConfigException.class, () -> topicService.createTopic(createOpts), "Expected ConfigException to throw");
        }
    }

    @ClusterTest
    public void testListTopics(ClusterInstance clusterInstance) throws InterruptedException {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin()) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);

            String output = captureListTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--list"));
            assertTrue(output.contains(testTopicName), "Expected topic name to be present in output: " + output);
        }
    }

    @ClusterTest(brokers = 3)
    public void testListTopicsWithIncludeList(ClusterInstance clusterInstance) throws InterruptedException {
        try (Admin adminClient = clusterInstance.admin()) {
            String topic1 = "kafka.testTopic1";
            String topic2 = "kafka.testTopic2";
            String topic3 = "oooof.testTopic1";
            int partition = 2;
            short replicationFactor = 2;
            adminClient.createTopics(Collections.singletonList(new NewTopic(topic1, partition, replicationFactor)));
            adminClient.createTopics(Collections.singletonList(new NewTopic(topic2, partition, replicationFactor)));
            adminClient.createTopics(Collections.singletonList(new NewTopic(topic3, partition, replicationFactor)));
            clusterInstance.waitForTopic(topic1, partition);
            clusterInstance.waitForTopic(topic2, partition);
            clusterInstance.waitForTopic(topic3, partition);

            String output = captureListTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--list", "--topic", "kafka.*"));
            assertTrue(output.contains(topic1), "Expected topic name " + topic1 + " to be present in output: " + output);
            assertTrue(output.contains(topic2), "Expected topic name " + topic2 + " to be present in output: " + output);
            assertFalse(output.contains(topic3), "Do not expect topic name " + topic3 + " to be present in output: " + output);
        }
    }

    @ClusterTest(brokers = 3)
    public void testListTopicsWithExcludeInternal(ClusterInstance clusterInstance) throws InterruptedException {
        try (Admin adminClient = clusterInstance.admin()) {
            String topic1 = "kafka.testTopic1";
            String hiddenConsumerTopic = Topic.GROUP_METADATA_TOPIC_NAME;
            int partition = 2;
            short replicationFactor = 2;
            adminClient.createTopics(Collections.singletonList(new NewTopic(topic1, partition, replicationFactor)));
            clusterInstance.waitForTopic(topic1, partition);

            String output = captureListTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--list", "--exclude-internal"));
            assertTrue(output.contains(topic1), "Expected topic name " + topic1 + " to be present in output: " + output);
            assertFalse(output.contains(hiddenConsumerTopic), "Do not expect topic name " + hiddenConsumerTopic + " to be present in output: " + output);
        }
    }

    @ClusterTest(brokers = 3)
    public void testAlterPartitionCount(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            int partition = 2;
            short replicationFactor = 2;
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)));
            clusterInstance.waitForTopic(testTopicName, partition);
            topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName, "--partitions", "3"));

            TestUtils.waitForCondition(
                    () -> adminClient.listPartitionReassignments().reassignments().get().isEmpty(),
                    CLUSTER_WAIT_MS, testTopicName + String.format("reassignmet not finished after %s ms", CLUSTER_WAIT_MS)
            );

            TestUtils.waitForCondition(
                    () -> clusterInstance.brokers().values().stream().allMatch(
                            b -> b.metadataCache().getTopicPartitions(testTopicName).size() == 3),
                    TestUtils.DEFAULT_MAX_WAIT_MS, "Timeout waiting for new assignment propagating to broker");
            TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues().get(testTopicName).get();
            assertEquals(3, topicDescription.partitions().size(), "Expected partition count to be 3. Got: " + topicDescription.partitions().size());
        }
    }

    @ClusterTemplate("generate")
    public void testAlterAssignment(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            int partition = 2;
            short replicationFactor = 2;

            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)));
            clusterInstance.waitForTopic(testTopicName, partition);

            topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter",
                    "--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "3"));

            TestUtils.waitForCondition(
                    () -> adminClient.listPartitionReassignments().reassignments().get().isEmpty(),
                    CLUSTER_WAIT_MS, testTopicName + String.format("reassignmet not finished after %s ms", CLUSTER_WAIT_MS)
            );

            TestUtils.waitForCondition(
                    () -> clusterInstance.brokers().values().stream().allMatch(
                            b -> b.metadataCache().getTopicPartitions(testTopicName).size() == 3),
                    TestUtils.DEFAULT_MAX_WAIT_MS, "Timeout waiting for new assignment propagating to broker");

            TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues().get(testTopicName).get();
            assertEquals(3, topicDescription.partitions().size(), "Expected partition count to be 3. Got: " + topicDescription.partitions().size());
            List<Integer> partitionReplicas = getPartitionReplicas(topicDescription.partitions(), 2);
            assertEquals(Arrays.asList(4, 2), partitionReplicas, "Expected to have replicas 4,2. Got: " + partitionReplicas);

        }
    }

    @ClusterTest(brokers = 3)
    public void testAlterAssignmentWithMoreAssignmentThanPartitions(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {

            int partition = 2;
            short replicationFactor = 2;
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)));
            clusterInstance.waitForTopic(testTopicName, partition);

            assertThrows(ExecutionException.class,
                    () -> topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter",
                            "--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2,3:2", "--partitions", "3")),
                    "Expected to fail with ExecutionException");

        }
    }

    @ClusterTemplate("generate")
    public void testAlterAssignmentWithMorePartitionsThanAssignment(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            int partition = 2;
            short replicationFactor = 2;
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)));
            clusterInstance.waitForTopic(testTopicName, partition);

            assertThrows(ExecutionException.class,
                    () -> topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName,
                            "--replica-assignment", "5:3,3:1,4:2", "--partitions", "6")),
                    "Expected to fail with ExecutionException");

        }
    }

    @ClusterTest
    public void testAlterWithInvalidPartitionCount(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);

            assertThrows(ExecutionException.class,
                    () -> topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--partitions", "-1", "--topic", testTopicName)),
                    "Expected to fail with ExecutionException");
        }
    }

    @ClusterTest
    public void testAlterWhenTopicDoesntExist(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            // alter a topic that does not exist without --if-exists
            TopicCommand.TopicCommandOptions alterOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName, "--partitions", "1");
            assertThrows(IllegalArgumentException.class, () -> topicService.alterTopic(alterOpts), "Expected to fail with IllegalArgumentException");

        }
    }

    @ClusterTest
    public void testAlterWhenTopicDoesntExistWithIfExists(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        Admin adminClient = clusterInstance.admin();

        TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient);
        topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName, "--partitions", "1", "--if-exists"));
        adminClient.close();
        topicService.close();
    }

    @ClusterTemplate("generate")
    public void testCreateAlterTopicWithRackAware(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {

            Map<Integer, String> rackInfo = new HashMap<>();
            rackInfo.put(0, "rack1");
            rackInfo.put(1, "rack2");
            rackInfo.put(2, "rack2");
            rackInfo.put(3, "rack1");
            rackInfo.put(4, "rack3");
            rackInfo.put(5, "rack3");

            int numPartitions = 18;
            int replicationFactor = 3;
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, numPartitions, (short) replicationFactor)));
            clusterInstance.waitForTopic(testTopicName, numPartitions);

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
            TopicCommand.TopicCommandOptions alterOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter",
                    "--partitions", Integer.toString(alteredNumPartitions),
                    "--topic", testTopicName);
            topicService.alterTopic(alterOpts);

            TestUtils.waitForCondition(
                    () -> adminClient.listPartitionReassignments().reassignments().get().isEmpty(),
                    CLUSTER_WAIT_MS, testTopicName + String.format("reassignmet not finished after %s ms", CLUSTER_WAIT_MS)
            );
            TestUtils.waitForCondition(
                    () -> clusterInstance.brokers().values().stream().allMatch(p -> p.metadataCache().getTopicPartitions(testTopicName).size() == alteredNumPartitions),
                    TestUtils.DEFAULT_MAX_WAIT_MS, "Timeout waiting for new assignment propagating to broker");

            assignment = adminClient.describeTopics(Collections.singletonList(testTopicName))
                    .allTopicNames().get().get(testTopicName).partitions().stream()
                    .collect(Collectors.toMap(info -> info.partition(), info -> info.replicas().stream().map(Node::id).collect(Collectors.toList())));
            checkReplicaDistribution(assignment, rackInfo, rackInfo.size(), alteredNumPartitions, replicationFactor,
                    true, true, true);

        }
    }

    @ClusterTest(brokers = 3)
    public void testConfigPreservationAcrossPartitionAlteration(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {

            String cleanUpPolicy = "compact";
            HashMap<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanUpPolicy);
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor).configs(topicConfig)));
            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
            Config props = adminClient.describeConfigs(Collections.singleton(configResource)).all().get().get(configResource);
            assertNotNull(props.get(TopicConfig.CLEANUP_POLICY_CONFIG), "Properties after creation don't contain " + cleanUpPolicy);
            assertEquals(cleanUpPolicy, props.get(TopicConfig.CLEANUP_POLICY_CONFIG).value(), "Properties after creation have incorrect value");

            // modify the topic to add new partitions
            int numPartitionsModified = 3;
            TopicCommand.TopicCommandOptions alterOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter",
                    "--partitions", Integer.toString(numPartitionsModified), "--topic", testTopicName);
            topicService.alterTopic(alterOpts);

            TestUtils.waitForCondition(
                    () -> clusterInstance.brokers().values().stream().allMatch(p -> p.metadataCache().getTopicPartitions(testTopicName).size() == numPartitionsModified),
                    TestUtils.DEFAULT_MAX_WAIT_MS, "Timeout waiting for new assignment propagating to broker");

            Config newProps = adminClient.describeConfigs(Collections.singleton(configResource)).all().get().get(configResource);
            assertNotNull(newProps.get(TopicConfig.CLEANUP_POLICY_CONFIG), "Updated properties do not contain " + TopicConfig.CLEANUP_POLICY_CONFIG);
            assertEquals(cleanUpPolicy, newProps.get(TopicConfig.CLEANUP_POLICY_CONFIG).value(), "Updated properties have incorrect value");

        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
        }
    )
    public void testTopicDeletion(ClusterInstance clusterInstance) throws Exception {
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            String testTopicName = TestUtils.randomString(10);

            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);

            // delete the NormalTopic
            TopicCommand.TopicCommandOptions deleteOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", testTopicName);
            topicService.deleteTopic(deleteOpts);

            TestUtils.waitForCondition(
                    () -> adminClient.listTopics().listings().get().stream().noneMatch(topic -> topic.name().equals(testTopicName)),
                    CLUSTER_WAIT_MS, String.format("Delete topic fail in %s ms", CLUSTER_WAIT_MS)
            );
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
        }
    )
    public void testTopicWithCollidingCharDeletionAndCreateAgain(ClusterInstance clusterInstance) throws Exception {
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            // create the topic with colliding chars
            String topicWithCollidingChar = "test.a";
            adminClient.createTopics(Collections.singletonList(new NewTopic(topicWithCollidingChar, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(topicWithCollidingChar, defaultNumPartitions);

            // delete the topic
            TopicCommand.TopicCommandOptions deleteOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", topicWithCollidingChar);
            topicService.deleteTopic(deleteOpts);
            TestUtils.waitForCondition(
                    () -> adminClient.listTopics().listings().get().stream().noneMatch(topic -> topic.name().equals(topicWithCollidingChar)),
                        CLUSTER_WAIT_MS, String.format("Delete topic fail in %s ms", CLUSTER_WAIT_MS)
            );

            clusterInstance.waitTopicDeletion(topicWithCollidingChar);

            // recreate same topic
            adminClient.createTopics(Collections.singletonList(new NewTopic(topicWithCollidingChar, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(topicWithCollidingChar, defaultNumPartitions);
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
        }
    )
    public void testDeleteInternalTopic(ClusterInstance clusterInstance) throws Exception {
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {

            // create the offset topic
            adminClient.createTopics(Collections.singletonList(new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(Topic.GROUP_METADATA_TOPIC_NAME, defaultNumPartitions);

            // Try to delete the Topic.GROUP_METADATA_TOPIC_NAME which is allowed by default.
            // This is a difference between the new and the old command as the old one didn't allow internal topic deletion.
            // If deleting internal topics is not desired, ACLS should be used to control it.
            TopicCommand.TopicCommandOptions deleteOffsetTopicOpts =
                    buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", Topic.GROUP_METADATA_TOPIC_NAME);

            topicService.deleteTopic(deleteOffsetTopicOpts);
            TestUtils.waitForCondition(
                    () -> adminClient.listTopics().listings().get().stream().noneMatch(topic -> topic.name().equals(Topic.GROUP_METADATA_TOPIC_NAME)),
                    CLUSTER_WAIT_MS, String.format("Delete topic fail in %s ms", CLUSTER_WAIT_MS)
            );

        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
        }
    )
    public void testDeleteWhenTopicDoesntExist(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            // delete a topic that does not exist
            TopicCommand.TopicCommandOptions deleteOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", testTopicName);
            assertThrows(IllegalArgumentException.class, () -> topicService.deleteTopic(deleteOpts),
                    "Expected an exception when trying to delete a topic that does not exist.");
        }
    }

    @ClusterTest(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
            @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
        }
    )
    public void testDeleteWhenTopicDoesntExistWithIfExists(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
            topicService.deleteTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", testTopicName, "--if-exists"));
        }
    }

    @ClusterTemplate("generate")
    public void testDescribe(ClusterInstance clusterInstance) throws InterruptedException {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin()) {
            int partition = 2;
            short replicationFactor = 2;
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)));
            clusterInstance.waitForTopic(testTopicName, partition);

            String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName));
            String[] rows = output.split(System.lineSeparator());
            assertEquals(3, rows.length, "Expected 3 rows in output, got " + rows.length);
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)), "Row does not start with " + testTopicName + ". Row is: " + rows[0]);
        }
    }

    @ClusterTemplate("generate")
    public void testDescribeWithDescribeTopicPartitionsApi(ClusterInstance clusterInstance) throws InterruptedException {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin()) {

            List<NewTopic> topics = new ArrayList<>();
            topics.add(new NewTopic(testTopicName, 20, (short) 2));
            topics.add(new NewTopic("test-2", 41, (short) 2));
            topics.add(new NewTopic("test-3", 5, (short) 2));
            topics.add(new NewTopic("test-4", 5, (short) 2));
            topics.add(new NewTopic("test-5", 100, (short) 2));

            adminClient.createTopics(topics);
            clusterInstance.waitForTopic(testTopicName, 20);
            clusterInstance.waitForTopic("test-2", 41);
            clusterInstance.waitForTopic("test-3", 5);
            clusterInstance.waitForTopic("test-4", 5);
            clusterInstance.waitForTopic("test-5", 100);

            String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance,
                    "--describe", "--partition-size-limit-per-response=20", "--exclude-internal"));
            String[] rows = output.split("\n");

            assertEquals(176, rows.length, String.join("\n", rows));
            assertTrue(rows[2].contains("\tElr"), rows[2]);
            assertTrue(rows[2].contains("LastKnownElr"), rows[2]);

        }
    }

    @ClusterTest
    public void testDescribeWhenTopicDoesntExist(ClusterInstance clusterInstance) {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin()) {
            TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient);

            assertThrows(IllegalArgumentException.class,
                    () -> topicService.describeTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName)),
                    "Expected an exception when trying to describe a topic that does not exist.");
        }

    }

    @ClusterTest
    public void testDescribeWhenTopicDoesntExistWithIfExists(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin()) {
            TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient);

            topicService.describeTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName, "--if-exists"));

            topicService.close();
        }
    }

    @ClusterTest(brokers = 3)
    public void testDescribeUnavailablePartitions(ClusterInstance clusterInstance) throws InterruptedException {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin()) {
            int partitions = 3;
            short replicationFactor = 1;

            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor)));
            clusterInstance.waitForTopic(testTopicName, partitions);

            // check which partition is on broker 0 which we'll kill
            clusterInstance.shutdownBroker(0);
            assertEquals(2, clusterInstance.aliveBrokers().size());

            // wait until the topic metadata for the test topic is propagated to each alive broker
            clusterInstance.waitForTopic(testTopicName, 3);

            // grab the console output and assert
            String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName, "--unavailable-partitions"));
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)),
                    "Unexpected Topic " + rows[0] + " received. Expect " + String.format("Topic: %s", testTopicName));
            assertTrue(rows[0].contains("Leader: none\tReplicas: 0\tIsr:"),
                    "Rows did not contain 'Leader: none\tReplicas: 0\tIsr:'");

        }
    }

    @ClusterTest(brokers = 3)
    public void testDescribeUnderReplicatedPartitions(ClusterInstance clusterInstance) throws InterruptedException {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin()) {
            int partitions = 1;
            short replicationFactor = 3;
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor)));
            clusterInstance.waitForTopic(testTopicName, partitions);

            clusterInstance.shutdownBroker(0);
            Assertions.assertEquals(clusterInstance.aliveBrokers().size(), 2);

            TestUtils.waitForCondition(
                    () -> clusterInstance.aliveBrokers().values().stream().allMatch(
                            broker -> {
                                Optional<UpdateMetadataRequestData.UpdateMetadataPartitionState> partitionState =
                                        Optional.ofNullable(broker.metadataCache().getPartitionInfo(testTopicName, 0).getOrElse(null));
                                return partitionState.map(s -> FetchRequest.isValidBrokerId(s.leader())).orElse(false);
                            }
                    ), CLUSTER_WAIT_MS, String.format("Meta data propogation fail in %s ms", CLUSTER_WAIT_MS));

            String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--under-replicated-partitions"));
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)), String.format("Unexpected output: %s", rows[0]));
        }
    }


    @ClusterTest(brokers = 3)
    public void testDescribeUnderMinIsrPartitions(ClusterInstance clusterInstance) throws InterruptedException {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin()) {
            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3");
            int partitions = 1;
            short replicationFactor = 3;
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor).configs(topicConfig)));
            clusterInstance.waitForTopic(testTopicName, partitions);

            clusterInstance.shutdownBroker(0);
            assertEquals(2, clusterInstance.aliveBrokers().size());

            TestUtils.waitForCondition(
                    () -> clusterInstance.aliveBrokers().values().stream().allMatch(broker -> broker.metadataCache().getPartitionInfo(testTopicName, 0).get().isr().size() == 2),
                    CLUSTER_WAIT_MS, String.format("Timeout waiting for partition metadata propagating to brokers for %s topic", testTopicName)
            );

            String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--under-min-isr-partitions", "--exclude-internal"));
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)),
                    "Unexpected topic: " + rows[0]);
        }
    }

    @ClusterTemplate("generate")
    public void testDescribeUnderReplicatedPartitionsWhenReassignmentIsInProgress(ClusterInstance clusterInstance) throws ExecutionException, InterruptedException {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin();
             KafkaProducer<String, String> producer = createProducer(clusterInstance)) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);

            TopicPartition tp = new TopicPartition(testTopicName, 0);

            // Produce multiple batches.
            sendProducerRecords(testTopicName, producer, 10);
            sendProducerRecords(testTopicName, producer, 10);

            // Enable throttling. Note the broker config sets the replica max fetch bytes to `1` upon to minimize replication
            // throughput so the reassignment doesn't complete quickly.
            List<Integer> brokerIds = new ArrayList<>(clusterInstance.brokerIds());

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
            TestUtils.waitForCondition(
                    () -> !adminClient.listPartitionReassignments(Collections.singleton(tp)).reassignments().get()
                                    .get(tp).addingReplicas().isEmpty(), CLUSTER_WAIT_MS, "Reassignment didn't add the second node"
            );

            // describe the topic and test if it's under-replicated
            String simpleDescribeOutput = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName));
            String[] simpleDescribeOutputRows = simpleDescribeOutput.split(System.lineSeparator());
            assertTrue(simpleDescribeOutputRows[0].startsWith(String.format("Topic: %s", testTopicName)),
                    "Unexpected describe output: " + simpleDescribeOutputRows[0]);
            assertEquals(2, simpleDescribeOutputRows.length,
                    "Unexpected describe output length: " + simpleDescribeOutputRows.length);

            String underReplicatedOutput = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--under-replicated-partitions"));
            assertEquals("", underReplicatedOutput,
                    String.format("--under-replicated-partitions shouldn't return anything: '%s'", underReplicatedOutput));

            int maxRetries = 20;
            long pause = 100L;
            long waitTimeMs = maxRetries * pause;
            AtomicReference<PartitionReassignment> reassignmentsRef = new AtomicReference<>();

            TestUtils.waitForCondition(
                    () -> {
                        PartitionReassignment tempReassignments = adminClient.listPartitionReassignments(Collections.singleton(tp)).reassignments().get().get(tp);
                        reassignmentsRef.set(tempReassignments);
                        return reassignmentsRef.get() != null;
                    }, waitTimeMs, "Reassignments did not become non-null within the specified time"
            );

            assertFalse(reassignmentsRef.get().addingReplicas().isEmpty());

            ToolsTestUtils.removeReplicationThrottleForPartitions(adminClient, brokerIds, Collections.singleton(tp));
            TestUtils.waitForCondition(
                    () -> adminClient.listPartitionReassignments().reassignments().get().isEmpty(),
                    CLUSTER_WAIT_MS,  String.format("reassignmet not finished after %s ms", CLUSTER_WAIT_MS)
            );
        }
    }

    @ClusterTemplate("generate")
    public void testDescribeAtMinIsrPartitions(ClusterInstance clusterInstance) throws InterruptedException {
        String testTopicName = TestUtils.randomString(10);

        try (Admin adminClient = clusterInstance.admin()) {
            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");

            int partitions = 1;
            short replicationFactor = 6;

            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor).configs(topicConfig)));
            clusterInstance.waitForTopic(testTopicName, partitions);

            clusterInstance.shutdownBroker(0);
            clusterInstance.shutdownBroker(1);
            assertEquals(4, clusterInstance.aliveBrokers().size());

            TestUtils.waitForCondition(
                    () -> clusterInstance.aliveBrokers().values().stream().allMatch(broker -> broker.metadataCache().getPartitionInfo(testTopicName, 0).get().isr().size() == 4),
                    CLUSTER_WAIT_MS, String.format("Timeout waiting for partition metadata propagating to brokers for %s topic", testTopicName)
            );


            String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--at-min-isr-partitions", "--exclude-internal"));
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)),
                    "Unexpected output: " + rows[0]);
            assertEquals(1, rows.length);
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
    @ClusterTemplate("generate")
    public void testDescribeUnderMinIsrPartitionsMixed(ClusterInstance clusterInstance) throws InterruptedException {
        try (Admin adminClient = clusterInstance.admin()) {
            String underMinIsrTopic = "under-min-isr-topic";
            String notUnderMinIsrTopic = "not-under-min-isr-topic";
            String offlineTopic = "offline-topic";
            String fullyReplicatedTopic = "fully-replicated-topic";
            int partitions = 1;
            short replicationFactor = 6;

            List<NewTopic> newTopics = new ArrayList<>();

            Map<Integer, List<Integer>> fullyReplicatedReplicaAssignmentMap = new HashMap<>();
            fullyReplicatedReplicaAssignmentMap.put(0, Arrays.asList(1, 2, 3));

            Map<Integer, List<Integer>> offlineReplicaAssignmentMap = new HashMap<>();
            offlineReplicaAssignmentMap.put(0, Arrays.asList(0));

            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6");

            newTopics.add(new NewTopic(underMinIsrTopic, partitions, replicationFactor).configs(topicConfig));
            newTopics.add(new NewTopic(notUnderMinIsrTopic, partitions, replicationFactor));
            newTopics.add(new NewTopic(offlineTopic, offlineReplicaAssignmentMap));
            newTopics.add(new NewTopic(fullyReplicatedTopic, fullyReplicatedReplicaAssignmentMap));

            adminClient.createTopics(newTopics);
            for (NewTopic topioc: newTopics) {
                clusterInstance.waitForTopic(topioc.name(), partitions);
            }

            clusterInstance.shutdownBroker(0);
            Assertions.assertEquals(5, clusterInstance.aliveBrokers().size());

            TestUtils.waitForCondition(
                    () -> clusterInstance.aliveBrokers().values().stream().allMatch(broker ->
                            broker.metadataCache().getPartitionInfo(underMinIsrTopic, 0).get().isr().size() < 6 &&
                            broker.metadataCache().getPartitionInfo(offlineTopic, 0).get().leader() == MetadataResponse.NO_LEADER_ID),
                    CLUSTER_WAIT_MS, "Timeout waiting for partition metadata propagating to brokers for underMinIsrTopic topic"
            );

            TestUtils.waitForCondition(
                    () -> adminClient.listPartitionReassignments().reassignments().get().isEmpty(),
                    CLUSTER_WAIT_MS,  String.format("reassignmet not finished after %s ms", CLUSTER_WAIT_MS)
            );

            String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--under-min-isr-partitions", "--exclude-internal"));
            String[] rows = output.split(System.lineSeparator());
            assertTrue(rows[0].startsWith(String.format("Topic: %s", underMinIsrTopic)),
                    "Unexpected output: " + rows[0]);
            assertTrue(rows[1].startsWith(String.format("\tTopic: %s", offlineTopic)),
                    "Unexpected output: " + rows[1]);
            assertEquals(2, rows.length);

        }
    }

    @ClusterTest(brokers = 3)
    public void testDescribeReportOverriddenConfigs(ClusterInstance clusterInstance) throws InterruptedException {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin()) {
            String config = "file.delete.delay.ms=1000";
            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "1000");

            int partitions = 2;
            short replicationFactor = 2;

            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor).configs(topicConfig)));
            clusterInstance.waitForTopic(testTopicName, partitions);

            String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe"));
            assertTrue(output.contains(config), String.format("Describe output should have contained %s", config));
        }
    }

    @ClusterTest
    public void testDescribeAndListTopicsWithoutInternalTopics(ClusterInstance clusterInstance) throws InterruptedException {
        String testTopicName = TestUtils.randomString(10);
        try (Admin adminClient = clusterInstance.admin()) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));
            clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);

            // test describe
            String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--describe", "--exclude-internal"));
            assertTrue(output.contains(testTopicName),
                    String.format("Output should have contained %s", testTopicName));
            assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME),
                    "Output should not have contained " + Topic.GROUP_METADATA_TOPIC_NAME);

            // test list
            output = captureListTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--list", "--exclude-internal"));
            assertTrue(output.contains(testTopicName), String.format("Output should have contained %s", testTopicName));
            assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME),
                    "Output should not have contained " + Topic.GROUP_METADATA_TOPIC_NAME);
        }
    }

    @ClusterTest
    public void testDescribeDoesNotFailWhenListingReassignmentIsUnauthorized(ClusterInstance clusterInstance) throws Exception {
        String testTopicName = TestUtils.randomString(10);
        Admin adminClient = clusterInstance.admin();

        adminClient = spy(adminClient);

        ListPartitionReassignmentsResult result = AdminClientTestUtils.listPartitionReassignmentsResult(
                new ClusterAuthorizationException("Unauthorized"));

        doReturn(result).when(adminClient).listPartitionReassignments(
                Collections.singleton(new TopicPartition(testTopicName, 0))
        );
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)));
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions);

        String output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName));
        String[] rows = output.split(System.lineSeparator());
        assertEquals(2, rows.length, "Unexpected output: " + output);
        assertTrue(rows[0].startsWith(String.format("Topic: %s", testTopicName)), "Unexpected output: " + rows[0]);

        adminClient.close();
    }

    @ClusterTest(brokers = 3)
    public void testCreateWithTopicNameCollision(ClusterInstance clusterInstance) throws Exception {
        try (Admin adminClient = clusterInstance.admin();
             TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {

            String topic = "foo_bar";
            int partitions = 1;
            short replicationFactor = 3;
            adminClient.createTopics(Collections.singletonList(new NewTopic(topic, partitions, replicationFactor)));
            clusterInstance.waitForTopic(topic, defaultNumPartitions);

            assertThrows(TopicExistsException.class,
                    () -> topicService.createTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--topic", topic)));

        }
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
        assignment.forEach((partition, assignedNodes) -> assertEquals(new HashSet<>(assignedNodes).size(), assignedNodes.size(),
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

    private String captureDescribeTopicStandardOut(ClusterInstance clusterInstance, TopicCommand.TopicCommandOptions opts) {
        Runnable runnable = () -> {
            try (Admin adminClient = clusterInstance.admin();
                 TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
                topicService.describeTopic(opts);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return ToolsTestUtils.captureStandardOut(runnable);
    }

    private String captureListTopicStandardOut(ClusterInstance clusterInstance, TopicCommand.TopicCommandOptions opts) {
        Runnable runnable = () -> {
            try (Admin adminClient = clusterInstance.admin();
                 TopicCommand.TopicService topicService = new TopicCommand.TopicService(adminClient)) {
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

        assignment.forEach((partitionId, replicaList) -> {
            Integer leader = replicaList.get(0);
            leaderCount.put(leader, leaderCount.getOrDefault(leader, 0) + 1);
            replicaList.forEach(brokerId -> {
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

    private KafkaProducer<String, String> createProducer(ClusterInstance clusterInstance) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
        return new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
    }

    private void sendProducerRecords(String testTopicName, KafkaProducer<String, String> producer, int numMessage) {
        IntStream.range(0, numMessage).forEach(i -> producer.send(new ProducerRecord<>(testTopicName, "test-" + i)));
        producer.flush();
    }
}
