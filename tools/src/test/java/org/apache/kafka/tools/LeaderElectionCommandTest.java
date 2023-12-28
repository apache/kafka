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

import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.collection.JavaConverters;

import org.mockito.MockedStatic;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@SuppressWarnings("deprecation")
@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(clusterType = Type.ALL, brokers = 3)
@Tag("integration")
public class LeaderElectionCommandTest {
    private final ClusterInstance cluster;
    int broker2 = 1;
    int broker3 = 2;

    public LeaderElectionCommandTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    void setup(ClusterConfig clusterConfig) {
        TestUtils.verifyNoUnexpectedThreads("@BeforeEach");
        clusterConfig.serverProperties().put("auto.leader.rebalance.enable", "false");
        clusterConfig.serverProperties().put("controlled.shutdown.enable", "true");
        clusterConfig.serverProperties().put("controlled.shutdown.max.retries", "1");
        clusterConfig.serverProperties().put("controlled.shutdown.retry.backoff.ms", "1000");
        clusterConfig.serverProperties().put("offsets.topic.replication.factor", "2");
    }

    @ClusterTest
    public void testAllTopicPartition() throws InterruptedException, ExecutionException {
        String topic = "unclean-topic";
        int partition = 0;
        List<Integer> assignment = Arrays.asList(broker2, broker3);

        cluster.waitForReadyBrokers();
        Admin client = cluster.createAdminClient();

        createTopic(client, topic, Collections.singletonMap(partition, assignment));

        TopicPartition topicPartition = new TopicPartition(topic, partition);

        TestUtils.assertLeader(client, topicPartition, broker2);
        cluster.shutdownBroker(broker3);
        TestUtils.waitForBrokersOutOfIsr(client,
                JavaConverters.asScalaBuffer(Collections.singletonList(topicPartition)).toSet(),
                JavaConverters.asScalaBuffer(Collections.singletonList(broker3)).toSet()
        );
        cluster.shutdownBroker(broker2);
        TestUtils.assertNoLeader(client, topicPartition);
        cluster.startBroker(broker3);
        TestUtils.waitForOnlineBroker(client, broker3);

        LeaderElectionCommand.main(
            "--bootstrap-server", cluster.bootstrapServers(),
            "--election-type", "unclean",
            "--all-topic-partitions"
        );

        TestUtils.assertLeader(client, topicPartition, broker3);
    }

    @ClusterTest
    public void testAdminConfigCustomTimeouts() throws Exception {
        String defaultApiTimeoutMs = String.valueOf(110000);
        String requestTimeoutMs = String.valueOf(55000);
        Path adminConfigPath = tempAdminConfig(defaultApiTimeoutMs, requestTimeoutMs);

        try (final MockedStatic<Admin> mockedAdmin = Mockito.mockStatic(Admin.class)) {
            LeaderElectionCommand.main(
                "--bootstrap-server", cluster.bootstrapServers(),
                "--election-type", "unclean", "--all-topic-partitions",
                "--admin.config", adminConfigPath.toString()
            );

            ArgumentCaptor<Properties> argumentCaptor = ArgumentCaptor.forClass(Properties.class);
            mockedAdmin.verify(() -> Admin.create(argumentCaptor.capture()));

            // verify that properties provided to admin client are the overridden properties
            final Properties actualProps = argumentCaptor.getValue();
            assertEquals(actualProps.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG), requestTimeoutMs);
            assertEquals(actualProps.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), defaultApiTimeoutMs);
        }
    }

    @ClusterTest
    public void testTopicPartition() throws InterruptedException, ExecutionException {
        String topic = "unclean-topic";
        int partition = 0;
        List<Integer> assignment = Arrays.asList(broker2, broker3);

        cluster.waitForReadyBrokers();
        Admin client = cluster.createAdminClient();
        createTopic(client, topic, Collections.singletonMap(partition, assignment));

        TopicPartition topicPartition = new TopicPartition(topic, partition);

        TestUtils.assertLeader(client, topicPartition, broker2);

        cluster.shutdownBroker(broker3);
        TestUtils.waitForBrokersOutOfIsr(client,
            JavaConverters.asScalaBuffer(Collections.singletonList(topicPartition)).toSet(),
            JavaConverters.asScalaBuffer(Collections.singletonList(broker3)).toSet()
        );
        cluster.shutdownBroker(broker2);
        TestUtils.assertNoLeader(client, topicPartition);
        cluster.startBroker(broker3);
        TestUtils.waitForOnlineBroker(client, broker3);

        LeaderElectionCommand.main(
            "--bootstrap-server", cluster.bootstrapServers(),
            "--election-type", "unclean",
            "--topic", topic,
            "--partition", Integer.toString(partition)
        );

        TestUtils.assertLeader(client, topicPartition, broker3);
    }

    @ClusterTest
    public void testPathToJsonFile() throws Exception {
        String topic = "unclean-topic";
        int partition = 0;
        List<Integer> assignment = Arrays.asList(broker2, broker3);

        cluster.waitForReadyBrokers();
        Map<Integer, List<Integer>> partitionAssignment = new HashMap<>();
        partitionAssignment.put(partition, assignment);

        Admin client = cluster.createAdminClient();
        createTopic(client, topic, partitionAssignment);

        TopicPartition topicPartition = new TopicPartition(topic, partition);

        TestUtils.assertLeader(client, topicPartition, broker2);

        cluster.shutdownBroker(broker3);
        TestUtils.waitForBrokersOutOfIsr(client,
            JavaConverters.asScalaBuffer(Collections.singletonList(topicPartition)).toSet(),
            JavaConverters.asScalaBuffer(Collections.singletonList(broker3)).toSet()
        );
        cluster.shutdownBroker(broker2);
        TestUtils.assertNoLeader(client, topicPartition);
        cluster.startBroker(broker3);
        TestUtils.waitForOnlineBroker(client, broker3);

        Path topicPartitionPath = tempTopicPartitionFile(Collections.singletonList(topicPartition));

        LeaderElectionCommand.main(
            "--bootstrap-server", cluster.bootstrapServers(),
            "--election-type", "unclean",
            "--path-to-json-file", topicPartitionPath.toString()
        );

        TestUtils.assertLeader(client, topicPartition, broker3);
    }

    @ClusterTest
    public void testPreferredReplicaElection() throws InterruptedException, ExecutionException {
        String topic = "preferred-topic";
        int partition = 0;
        List<Integer> assignment = Arrays.asList(broker2, broker3);

        cluster.waitForReadyBrokers();
        Admin client = cluster.createAdminClient();
        Map<Integer, List<Integer>> partitionAssignment = new HashMap<>();
        partitionAssignment.put(partition, assignment);

        createTopic(client, topic, partitionAssignment);

        TopicPartition topicPartition = new TopicPartition(topic, partition);

        TestUtils.assertLeader(client, topicPartition, broker2);

        cluster.shutdownBroker(broker2);
        TestUtils.assertLeader(client, topicPartition, broker3);
        cluster.startBroker(broker2);
        TestUtils.waitForBrokersInIsr(client, topicPartition,
            JavaConverters.asScalaBuffer(Collections.singletonList(broker2)).toSet()
        );

        LeaderElectionCommand.main(
            "--bootstrap-server", cluster.bootstrapServers(),
            "--election-type", "preferred",
            "--all-topic-partitions"
        );

        TestUtils.assertLeader(client, topicPartition, broker2);
    }

    @ClusterTest
    public void testTopicDoesNotExist() {
        Throwable e =  assertThrows(AdminCommandFailedException.class, () -> LeaderElectionCommand.run(
            Duration.ofSeconds(30),
            "--bootstrap-server", cluster.bootstrapServers(),
            "--election-type", "preferred",
            "--topic", "unknown-topic-name",
            "--partition", "0"
        ));
        assertTrue(e.getSuppressed()[0] instanceof UnknownTopicOrPartitionException);
    }

    @ClusterTest
    public void testElectionResultOutput() throws Exception {
        String topic = "non-preferred-topic";
        int partition0 = 0;
        int partition1 = 1;
        List<Integer> assignment0 = Arrays.asList(broker2, broker3);
        List<Integer> assignment1 = Arrays.asList(broker3, broker2);

        cluster.waitForReadyBrokers();
        Admin client = cluster.createAdminClient();
        Map<Integer, List<Integer>> partitionAssignment = new HashMap<>();
        partitionAssignment.put(partition0, assignment0);
        partitionAssignment.put(partition1, assignment1);

        createTopic(client, topic, partitionAssignment);

        TopicPartition topicPartition0 = new TopicPartition(topic, partition0);
        TopicPartition topicPartition1 = new TopicPartition(topic, partition1);

        TestUtils.assertLeader(client, topicPartition0, broker2);
        TestUtils.assertLeader(client, topicPartition1, broker3);

        cluster.shutdownBroker(broker2);
        TestUtils.assertLeader(client, topicPartition0, broker3);
        cluster.startBroker(broker2);
        TestUtils.waitForBrokersInIsr(client, topicPartition0,
            JavaConverters.asScalaBuffer(Collections.singletonList(broker2)).toSet()
        );
        TestUtils.waitForBrokersInIsr(client, topicPartition1,
            JavaConverters.asScalaBuffer(Collections.singletonList(broker2)).toSet()
        );

        Path topicPartitionPath = tempTopicPartitionFile(Arrays.asList(topicPartition0, topicPartition1));
        String output = ToolsTestUtils.captureStandardOut(() ->
            LeaderElectionCommand.main(
                "--bootstrap-server", cluster.bootstrapServers(),
                "--election-type", "preferred",
                "--path-to-json-file", topicPartitionPath.toString()
            ));

        Iterator<String> electionResultOutputIter = Arrays.stream(output.split("\n")).iterator();

        assertTrue(electionResultOutputIter.hasNext());
        String firstLine = electionResultOutputIter.next();
        assertTrue(firstLine.contains(String.format(
            "Successfully completed leader election (PREFERRED) for partitions %s", topicPartition0)),
            String.format("Unexpected output: %s", firstLine));

        assertTrue(electionResultOutputIter.hasNext());
        String secondLine = electionResultOutputIter.next();
        assertTrue(secondLine.contains(String.format("Valid replica already elected for partitions %s", topicPartition1)),
            String.format("Unexpected output: %s", secondLine));
    }

    private static void createTopic(Admin admin, String topic, Map<Integer, List<Integer>> replicaAssignment) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topic, replicaAssignment);
        List<NewTopic> newTopics = Collections.singletonList(newTopic);
        CreateTopicsResult createTopicResult = admin.createTopics(newTopics);
        createTopicResult.all().get();
    }

    private static Path tempTopicPartitionFile(List<TopicPartition> partitions) throws Exception {
        java.io.File file = TestUtils.tempFile("leader-election-command", ".json");

        scala.collection.immutable.Set<TopicPartition> topicPartitionSet =
            JavaConverters.asScalaBuffer(partitions).toSet();
        String jsonString = TestUtils.stringifyTopicPartitions(topicPartitionSet);

        Files.write(file.toPath(), jsonString.getBytes(StandardCharsets.UTF_8));

        return file.toPath();
    }
    private static Path tempAdminConfig(String defaultApiTimeoutMs, String requestTimeoutMs) throws Exception {
        String content = "default.api.timeout.ms=" + defaultApiTimeoutMs + "\nrequest.timeout.ms=" + requestTimeoutMs;
        java.io.File file = TestUtils.tempFile("admin-config", ".properties");
        Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return file.toPath();
    }
}
