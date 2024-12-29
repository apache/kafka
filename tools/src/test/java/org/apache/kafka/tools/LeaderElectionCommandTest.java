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

import kafka.utils.TestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.server.common.AdminCommandFailedException;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import scala.jdk.javaapi.CollectionConverters;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(brokers = 3, serverProperties = {
    @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
    @ClusterConfigProperty(key = "auto.leader.rebalance.enable", value = "false"),
    @ClusterConfigProperty(key = "controlled.shutdown.enable", value = "true"),
    @ClusterConfigProperty(key = "controlled.shutdown.max.retries", value = "1"),
    @ClusterConfigProperty(key = "controlled.shutdown.retry.backoff.ms", value = "1000"),
    @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "2")
})
public class LeaderElectionCommandTest {
    private final ClusterInstance cluster;
    int broker2 = 1;
    int broker3 = 2;

    public LeaderElectionCommandTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testAllTopicPartition() throws InterruptedException, ExecutionException {
        String topic = "unclean-topic";
        int partition = 0;
        List<Integer> assignment = asList(broker2, broker3);

        cluster.waitForReadyBrokers();
        try (Admin client = cluster.admin()) {

            createTopic(client, topic, Collections.singletonMap(partition, assignment));

            TopicPartition topicPartition = new TopicPartition(topic, partition);

            TestUtils.assertLeader(client, topicPartition, broker2);
            cluster.shutdownBroker(broker3);
            TestUtils.waitForBrokersOutOfIsr(client,
                    CollectionConverters.asScala(singletonList(topicPartition)).toSet(),
                    CollectionConverters.asScala(singletonList(broker3)).toSet()
            );
            cluster.shutdownBroker(broker2);
            TestUtils.assertNoLeader(client, topicPartition);
            cluster.startBroker(broker3);
            TestUtils.waitForOnlineBroker(client, broker3);

            assertEquals(0, LeaderElectionCommand.mainNoExit(
                    "--bootstrap-server", cluster.bootstrapServers(),
                    "--election-type", "unclean",
                    "--all-topic-partitions"
            ));

            TestUtils.assertLeader(client, topicPartition, broker3);
        }
    }

    @ClusterTest
    public void testAdminConfigCustomTimeouts() throws Exception {
        String defaultApiTimeoutMs = String.valueOf(110000);
        String requestTimeoutMs = String.valueOf(55000);
        Path adminConfigPath = tempAdminConfig(defaultApiTimeoutMs, requestTimeoutMs);

        try (final MockedStatic<Admin> mockedAdmin = Mockito.mockStatic(Admin.class)) {
            assertEquals(1, LeaderElectionCommand.mainNoExit(
                "--bootstrap-server", cluster.bootstrapServers(),
                "--election-type", "unclean", "--all-topic-partitions",
                "--admin.config", adminConfigPath.toString()
            ));

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
        List<Integer> assignment = asList(broker2, broker3);

        cluster.waitForReadyBrokers();
        try (Admin client = cluster.admin()) {
            createTopic(client, topic, Collections.singletonMap(partition, assignment));

            TopicPartition topicPartition = new TopicPartition(topic, partition);

            TestUtils.assertLeader(client, topicPartition, broker2);

            cluster.shutdownBroker(broker3);
            TestUtils.waitForBrokersOutOfIsr(client,
                    CollectionConverters.asScala(singletonList(topicPartition)).toSet(),
                    CollectionConverters.asScala(singletonList(broker3)).toSet()
            );
            cluster.shutdownBroker(broker2);
            TestUtils.assertNoLeader(client, topicPartition);
            cluster.startBroker(broker3);
            TestUtils.waitForOnlineBroker(client, broker3);

            assertEquals(0, LeaderElectionCommand.mainNoExit(
                    "--bootstrap-server", cluster.bootstrapServers(),
                    "--election-type", "unclean",
                    "--topic", topic,
                    "--partition", Integer.toString(partition)
            ));

            TestUtils.assertLeader(client, topicPartition, broker3);
        }
    }

    @ClusterTest
    public void testPathToJsonFile() throws Exception {
        String topic = "unclean-topic";
        int partition = 0;
        List<Integer> assignment = asList(broker2, broker3);

        cluster.waitForReadyBrokers();
        Map<Integer, List<Integer>> partitionAssignment = new HashMap<>();
        partitionAssignment.put(partition, assignment);

        try (Admin client = cluster.admin()) {
            createTopic(client, topic, partitionAssignment);

            TopicPartition topicPartition = new TopicPartition(topic, partition);

            TestUtils.assertLeader(client, topicPartition, broker2);

            cluster.shutdownBroker(broker3);
            TestUtils.waitForBrokersOutOfIsr(client,
                    CollectionConverters.asScala(singletonList(topicPartition)).toSet(),
                    CollectionConverters.asScala(singletonList(broker3)).toSet()
            );
            cluster.shutdownBroker(broker2);
            TestUtils.assertNoLeader(client, topicPartition);
            cluster.startBroker(broker3);
            TestUtils.waitForOnlineBroker(client, broker3);

            Path topicPartitionPath = tempTopicPartitionFile(singletonList(topicPartition));

            assertEquals(0, LeaderElectionCommand.mainNoExit(
                    "--bootstrap-server", cluster.bootstrapServers(),
                    "--election-type", "unclean",
                    "--path-to-json-file", topicPartitionPath.toString()
            ));

            TestUtils.assertLeader(client, topicPartition, broker3);
        }
    }

    @ClusterTest
    public void testPreferredReplicaElection() throws InterruptedException, ExecutionException {
        String topic = "preferred-topic";
        int partition = 0;
        List<Integer> assignment = asList(broker2, broker3);

        cluster.waitForReadyBrokers();
        try (Admin client = cluster.admin()) {
            Map<Integer, List<Integer>> partitionAssignment = new HashMap<>();
            partitionAssignment.put(partition, assignment);

            createTopic(client, topic, partitionAssignment);

            TopicPartition topicPartition = new TopicPartition(topic, partition);

            TestUtils.assertLeader(client, topicPartition, broker2);

            cluster.shutdownBroker(broker2);
            TestUtils.assertLeader(client, topicPartition, broker3);
            cluster.startBroker(broker2);
            TestUtils.waitForBrokersInIsr(client, topicPartition,
                    CollectionConverters.asScala(singletonList(broker2)).toSet()
            );

            assertEquals(0, LeaderElectionCommand.mainNoExit(
                    "--bootstrap-server", cluster.bootstrapServers(),
                    "--election-type", "preferred",
                    "--all-topic-partitions"
            ));

            TestUtils.assertLeader(client, topicPartition, broker2);
        }
    }

    @ClusterTest
    public void testTopicDoesNotExist() {
        Throwable e = assertThrows(AdminCommandFailedException.class, () -> LeaderElectionCommand.run(
            Duration.ofSeconds(30),
            "--bootstrap-server", cluster.bootstrapServers(),
            "--election-type", "preferred",
            "--topic", "unknown-topic-name",
            "--partition", "0"
        ));
        assertInstanceOf(UnknownTopicOrPartitionException.class, e.getSuppressed()[0]);
    }

    @ClusterTest
    public void testElectionResultOutput() throws Exception {
        String topic = "non-preferred-topic";
        int partition0 = 0;
        int partition1 = 1;
        List<Integer> assignment0 = asList(broker2, broker3);
        List<Integer> assignment1 = asList(broker3, broker2);

        cluster.waitForReadyBrokers();
        TopicPartition topicPartition0;
        TopicPartition topicPartition1;
        try (Admin client = cluster.admin()) {
            Map<Integer, List<Integer>> partitionAssignment = new HashMap<>();
            partitionAssignment.put(partition0, assignment0);
            partitionAssignment.put(partition1, assignment1);

            createTopic(client, topic, partitionAssignment);

            topicPartition0 = new TopicPartition(topic, partition0);
            topicPartition1 = new TopicPartition(topic, partition1);

            TestUtils.assertLeader(client, topicPartition0, broker2);
            TestUtils.assertLeader(client, topicPartition1, broker3);

            cluster.shutdownBroker(broker2);
            TestUtils.assertLeader(client, topicPartition0, broker3);
            cluster.startBroker(broker2);
            TestUtils.waitForBrokersInIsr(client, topicPartition0,
                    CollectionConverters.asScala(singletonList(broker2)).toSet()
            );
            TestUtils.waitForBrokersInIsr(client, topicPartition1,
                    CollectionConverters.asScala(singletonList(broker2)).toSet()
            );
        }

        Path topicPartitionPath = tempTopicPartitionFile(asList(topicPartition0, topicPartition1));
        String output = ToolsTestUtils.captureStandardOut(() ->
            LeaderElectionCommand.mainNoExit(
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

    private void createTopic(Admin admin, String topic, Map<Integer, List<Integer>> replicaAssignment) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topic, replicaAssignment);
        List<NewTopic> newTopics = singletonList(newTopic);
        CreateTopicsResult createTopicResult = admin.createTopics(newTopics);
        createTopicResult.all().get();
    }

    private Path tempTopicPartitionFile(List<TopicPartition> partitions) throws Exception {
        java.io.File file = TestUtils.tempFile("leader-election-command", ".json");

        String jsonString = stringifyTopicPartitions(new HashSet<>(partitions));

        Files.write(file.toPath(), jsonString.getBytes(StandardCharsets.UTF_8));

        return file.toPath();
    }

    private Path tempAdminConfig(String defaultApiTimeoutMs, String requestTimeoutMs) throws Exception {
        String content = "default.api.timeout.ms=" + defaultApiTimeoutMs + "\nrequest.timeout.ms=" + requestTimeoutMs;
        java.io.File file = TestUtils.tempFile("admin-config", ".properties");
        Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return file.toPath();
    }

    private String stringifyTopicPartitions(Set<TopicPartition> topicPartitions) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"partitions\":[");
        Iterator<TopicPartition> iterator = topicPartitions.iterator();
        while (iterator.hasNext()) {
            TopicPartition topicPartition = iterator.next();
            sb.append("{\"topic\":\"")
                    .append(topicPartition.topic())
                    .append("\",\"partition\":")
                    .append(topicPartition.partition()).append("}");
            if (iterator.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("]}");
        return sb.toString();
    }
}
