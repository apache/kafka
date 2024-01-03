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

import kafka.utils.Exit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@Timeout(value = 60)
public class TopicCommandTest {
    private String bootstrapServer = "localhost:9092";
    private String topicName = "topicName";

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
        assertInitializeInvalidOptionsExitCode(1,
            new String[] {"--bootstrap-server", bootstrapServer, "--alter", "--topic", topicName,
                "--partitions", "3", "--delete-config", "cleanup.policy"});
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

        assertTrue(exception.getCause() instanceof ThrottlingQuotaExceededException);

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
        assertTrue(exception.getCause() instanceof ThrottlingQuotaExceededException);

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
}
