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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import joptsimple.OptionException;

import static java.time.LocalDateTime.now;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases by:
 * - Non-existing consumer group
 * - One for each scenario, with scope=all-topics
 * - scope=one topic, scenario=to-earliest
 * - scope=one topic+partitions, scenario=to-earliest
 * - scope=topics, scenario=to-earliest
 * - scope=topics+partitions, scenario=to-earliest
 * - export/import
 */
@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "1000"),
        @ClusterConfigProperty(key = CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
        @ClusterConfigProperty(key = CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
    }
)
public class ResetConsumerGroupOffsetTest {

    private static final String TOPIC_PREFIX = "foo-";
    private static final String GROUP_PREFIX = "test.group-";

    private String[] basicArgs(ClusterInstance cluster) {
        return new String[]{"--reset-offsets",
            "--bootstrap-server", cluster.bootstrapServers(),
            "--timeout", Long.toString(DEFAULT_MAX_WAIT_MS)};
    }

    private String[] buildArgsForGroups(ClusterInstance cluster, List<String> groups, String... args) {
        List<String> res = new ArrayList<>(List.of(basicArgs(cluster)));
        for (String group : groups) {
            res.add("--group");
            res.add(group);
        }
        res.addAll(List.of(args));
        return res.toArray(new String[0]);
    }

    private String[] buildArgsForGroup(ClusterInstance cluster, String group, String... args) {
        return buildArgsForGroups(cluster, List.of(group), args);
    }

    private String[] buildArgsForAllGroups(ClusterInstance cluster, String... args) {
        List<String> res = new ArrayList<>(List.of(basicArgs(cluster)));
        res.add("--all-groups");
        res.addAll(List.of(args));
        return res.toArray(new String[0]);
    }

    @ClusterTest
    public void testResetOffsetsNotExistingGroup(ClusterInstance cluster) throws Exception {
        String topic = generateRandomTopic();
        String group = "missing.group";
        String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--to-current", "--execute");

        try (ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(args)) {
            Map<TopicPartition, OffsetAndMetadata> resetOffsets = service.resetOffsets().get(group);
            assertTrue(resetOffsets.isEmpty());
            assertTrue(committedOffsets(cluster, topic, group).isEmpty());
        }
    }

    @ClusterTest(
        brokers = 2,
        serverProperties = {
            @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2"),
        }
    )
    public void testResetOffsetsWithOfflinePartitionNotInResetTarget(ClusterInstance cluster) throws Exception {
        String topic = generateRandomTopic();
        String group = "new.group";
        String[] args = buildArgsForGroup(cluster, group, "--to-earliest", "--execute", "--topic", topic + ":0");

        try (Admin admin = cluster.admin(); ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(args)) {
            admin.createTopics(List.of(new NewTopic(topic, Map.of(0, List.of(0), 1, List.of(1)))));
            cluster.waitTopicCreation(topic, 2);

            cluster.shutdownBroker(1);

            Map<TopicPartition, OffsetAndMetadata> resetOffsets = service.resetOffsets().get(group);
            assertEquals(Set.of(new TopicPartition(topic, 0)), resetOffsets.keySet());
        }
    }

    @ClusterTest
    public void testResetOffsetsExistingTopic(ClusterInstance cluster) {
        String topic = generateRandomTopic();
        String group = "new.group";
        String[] args = buildArgsForGroup(cluster, group, "--topic", topic, "--to-offset", "50");

        produceMessages(cluster, topic, 100);
        resetAndAssertOffsets(cluster, args, 50, true, List.of(topic));
        resetAndAssertOffsets(cluster, addTo(args, "--dry-run"),
                50, true, List.of(topic));
        resetAndAssertOffsets(cluster, addTo(args, "--execute"),
                50, false, List.of(topic));
    }

    @ClusterTest
    public void testResetOffsetsExistingTopicSelectedGroups(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String topic = generateRandomTopic();

            produceMessages(cluster, topic, 100);
            List<String> groups = generateIds(topic);
            for (String group : groups) {
                try (AutoCloseable consumerGroupCloseable =
                             consumerGroupClosable(cluster, 1, topic, group, groupProtocol)) {
                    awaitConsumerProgress(cluster, topic, group, 100L);
                }
            }

            String[] args = buildArgsForGroups(cluster, groups, "--topic", topic, "--to-offset", "50");
            resetAndAssertOffsets(cluster, args, 50, true, List.of(topic));
            resetAndAssertOffsets(cluster, addTo(args, "--dry-run"),
                    50, true, List.of(topic));
            resetAndAssertOffsets(cluster, addTo(args, "--execute"),
                    50, false, List.of(topic));
        }
    }

    @ClusterTest
    public void testResetOffsetsExistingTopicAllGroups(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String topic = generateRandomTopic();
            String[] args = buildArgsForAllGroups(cluster, "--topic", topic, "--to-offset", "50");

            produceMessages(cluster, topic, 100);
            for (int i = 1; i <= 3; i++) {
                String group = generateRandomGroupId();
                try (AutoCloseable consumerGroupCloseable =
                             consumerGroupClosable(cluster, 1, topic, group, groupProtocol)) {
                    awaitConsumerProgress(cluster, topic, group, 100L);
                }
            }
            resetAndAssertOffsets(cluster, args, 50, true, List.of(topic));
            resetAndAssertOffsets(cluster, addTo(args, "--dry-run"),
                    50, true, List.of(topic));
            resetAndAssertOffsets(cluster, addTo(args, "--execute"),
                    50, false, List.of(topic));
        }
    }

    @ClusterTest
    public void testResetOffsetsAllTopicsAllGroups(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String groupId = generateRandomGroupId();
            String topicId = generateRandomTopic();

            String[] args = buildArgsForAllGroups(cluster, "--all-topics", "--to-offset", "50");
            List<String> topics = generateIds(groupId);
            List<String> groups = generateIds(topicId);
            topics.forEach(topic -> produceMessages(cluster, topic, 100));

            for (String topic : topics) {
                for (String group : groups) {
                    try (AutoCloseable consumerGroupCloseable =
                                 consumerGroupClosable(cluster, 3, topic, group, groupProtocol)) {
                        awaitConsumerProgress(cluster, topic, group, 100);
                    }
                }
            }

            resetAndAssertOffsets(cluster, args, 50, true, topics);
            resetAndAssertOffsets(cluster, addTo(args, "--dry-run"),
                    50, true, topics);
            resetAndAssertOffsets(cluster, addTo(args, "--execute"),
                    50, false, topics);

            try (Admin admin = cluster.admin()) {
                admin.deleteConsumerGroups(groups).all().get();
            }
        }
    }

    @ClusterTest
    public void testResetOffsetsToLocalDateTime(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
            LocalDateTime dateTime = now().minusDays(1);
            String[] args = buildArgsForGroup(cluster, group,
                "--all-topics", "--to-datetime",
                format.format(dateTime), "--execute");

            produceMessages(cluster, topic, 100);

            try (AutoCloseable consumerGroupCloseable =
                         consumerGroupClosable(cluster, 1, topic, group, groupProtocol)) {
                awaitConsumerProgress(cluster, topic, group, 100L);
            }

            resetAndAssertOffsets(cluster, topic, args, 0);
        }
    }

    @ClusterTest
    public void testResetOffsetsToZonedDateTime(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();
            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

            produceMessages(cluster, topic, 50);
            ZonedDateTime checkpoint = now().atZone(ZoneId.systemDefault());
            produceMessages(cluster, topic, 50);

            String[] args = buildArgsForGroup(cluster, group,
                    "--all-topics", "--to-datetime", format.format(checkpoint),
                    "--execute");

            try (AutoCloseable consumerGroupCloseable =
                         consumerGroupClosable(cluster, 1, topic, group, groupProtocol)) {
                awaitConsumerProgress(cluster, topic, group, 100L);
            }

            resetAndAssertOffsets(cluster, topic, args, 50);
        }
    }

    @ClusterTest
    public void testResetOffsetsByDuration(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--by-duration", "PT1M", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            resetAndAssertOffsets(cluster, topic, args, 0);
        }
    }

    @ClusterTest
    public void testResetOffsetsByDurationToEarliest(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--by-duration", "PT0.1S", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            resetAndAssertOffsets(cluster, topic, args, 100);
        }
    }

    @ClusterTest
    public void testResetOffsetsByDurationFallbackToLatestWhenNoRecords(ClusterInstance cluster) throws ExecutionException, InterruptedException {
        String group = generateRandomGroupId();
        String topic = generateRandomTopic();

        String[] args = buildArgsForGroup(cluster, group, "--topic", topic, "--by-duration", "PT1M", "--execute");

        try (Admin admin = cluster.admin()) {
            admin.createTopics(Set.of(new NewTopic(topic, 1, (short) 1))).all().get();
            resetAndAssertOffsets(cluster, args, 0, false, List.of(topic));
            admin.deleteTopics(Set.of(topic)).all().get();
        }
    }

    @ClusterTest
    public void testResetOffsetsToEarliest(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--to-earliest", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            resetAndAssertOffsets(cluster, topic, args, 0);
        }
    }

    @ClusterTest
    public void testResetOffsetsToLatest(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--to-latest", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            produceMessages(cluster, topic, 100);
            resetAndAssertOffsets(cluster, topic, args, 200);
        }
    }

    @ClusterTest
    public void testResetOffsetsToCurrentOffset(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--to-current", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            produceMessages(cluster, topic, 100);
            resetAndAssertOffsets(cluster, topic, args, 100);
        }
    }

    @ClusterTest
    public void testResetOffsetsToSpecificOffset(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--to-offset", "1", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            resetAndAssertOffsets(cluster, topic, args, 1);
        }
    }

    @ClusterTest
    public void testResetOffsetsShiftPlus(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--shift-by", "50", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            produceMessages(cluster, topic, 100);
            resetAndAssertOffsets(cluster, topic, args, 150);
        }
    }

    @ClusterTest
    public void testResetOffsetsShiftMinus(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--shift-by", "-50", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            produceMessages(cluster, topic, 100);
            resetAndAssertOffsets(cluster, topic, args, 50);
        }
    }

    @ClusterTest
    public void testResetOffsetsShiftByLowerThanEarliest(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--shift-by", "-150", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            produceMessages(cluster, topic, 100);
            resetAndAssertOffsets(cluster, topic, args, 0);
        }
    }

    @ClusterTest
    public void testResetOffsetsShiftByHigherThanLatest(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--all-topics", "--shift-by", "150", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            produceMessages(cluster, topic, 100);
            resetAndAssertOffsets(cluster, topic, args, 200);
        }
    }

    @ClusterTest
    public void testResetOffsetsToEarliestOnOneTopic(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            String[] args = buildArgsForGroup(cluster, group, "--topic", topic, "--to-earliest", "--execute");
            produceConsumeAndShutdown(cluster, topic, group, 1, groupProtocol);
            resetAndAssertOffsets(cluster, topic, args, 0);
        }
    }

    @ClusterTest
    public void testResetOffsetsToEarliestOnOneTopicAndPartition(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();
            String[] args = buildArgsForGroup(cluster, group, "--topic", topic + ":1",
                "--to-earliest", "--execute");

            try (Admin admin = cluster.admin();
                 ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(args)) {
                admin.createTopics(Set.of(new NewTopic(topic, 2, (short) 1))).all().get();

                produceConsumeAndShutdown(cluster, topic, group, 2, groupProtocol);
                Map<TopicPartition, Long> priorCommittedOffsets = committedOffsets(cluster, topic, group);
                TopicPartition tp0 = new TopicPartition(topic, 0);
                TopicPartition tp1 = new TopicPartition(topic, 1);
                Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
                expectedOffsets.put(tp0, priorCommittedOffsets.get(tp0));
                expectedOffsets.put(tp1, 0L);
                resetAndAssertOffsetsCommitted(cluster, service, expectedOffsets, topic);

                admin.deleteTopics(Set.of(topic)).all().get();
            }
        }
    }

    @ClusterTest
    public void testResetOffsetsToEarliestOnTopics(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic1 = generateRandomTopic();
            String topic2 = generateRandomTopic();
            String[] args = buildArgsForGroup(cluster, group,
                "--topic", topic1,
                "--topic", topic2,
                "--to-earliest", "--execute");

            try (Admin admin = cluster.admin();
                 ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(args)) {
                admin.createTopics(List.of(new NewTopic(topic1, 1, (short) 1),
                        new NewTopic(topic2, 1, (short) 1))).all().get();

                produceConsumeAndShutdown(cluster, topic1, group, 1, groupProtocol);
                produceConsumeAndShutdown(cluster, topic2, group, 1, groupProtocol);

                TopicPartition tp1 = new TopicPartition(topic1, 0);
                TopicPartition tp2 = new TopicPartition(topic2, 0);

                Map<TopicPartition, Long> allResetOffsets = toOffsetMap(resetOffsets(service).get(group));
                Map<TopicPartition, Long> expMap = new HashMap<>();
                expMap.put(tp1, 0L);
                expMap.put(tp2, 0L);
                assertEquals(expMap, allResetOffsets);
                assertEquals(Map.of(tp1, 0L), committedOffsets(cluster, topic1, group));
                assertEquals(Map.of(tp2, 0L), committedOffsets(cluster, topic2, group));

                admin.deleteTopics(List.of(topic1, topic2)).all().get();
            }
        }
    }

    @ClusterTest
    public void testResetOffsetsToEarliestOnTopicsAndPartitions(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic1 = generateRandomTopic();
            String topic2 = generateRandomTopic();
            String[] args = buildArgsForGroup(cluster, group,
                "--topic", topic1 + ":1",
                "--topic", topic2 + ":1",
                "--to-earliest", "--execute");

            try (Admin admin = cluster.admin();
                 ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(args)) {
                admin.createTopics(List.of(new NewTopic(topic1, 2, (short) 1),
                        new NewTopic(topic2, 2, (short) 1))).all().get();

                produceConsumeAndShutdown(cluster, topic1, group, 2, groupProtocol);
                produceConsumeAndShutdown(cluster, topic2, group, 2, groupProtocol);

                Map<TopicPartition, Long> priorCommittedOffsets1 =
                        committedOffsets(cluster, topic1, group);
                Map<TopicPartition, Long> priorCommittedOffsets2 =
                        committedOffsets(cluster, topic2, group);

                TopicPartition tp1 = new TopicPartition(topic1, 1);
                TopicPartition tp2 = new TopicPartition(topic2, 1);
                Map<TopicPartition, Long> allResetOffsets = toOffsetMap(resetOffsets(service).get(group));
                Map<TopicPartition, Long> expMap = new HashMap<>();
                expMap.put(tp1, 0L);
                expMap.put(tp2, 0L);
                assertEquals(expMap, allResetOffsets);
                priorCommittedOffsets1.put(tp1, 0L);
                assertEquals(priorCommittedOffsets1, committedOffsets(cluster, topic1, group));
                priorCommittedOffsets2.put(tp2, 0L);
                assertEquals(priorCommittedOffsets2, committedOffsets(cluster, topic2, group));

                admin.deleteTopics(List.of(topic1, topic2)).all().get();
            }
        }
    }

    @ClusterTest
    // This one deals with old CSV export/import format for a single --group arg:
    // "topic,partition,offset" to support old behavior
    public void testResetOffsetsExportImportPlanSingleGroupArg(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group = generateRandomGroupId();
            String topic = generateRandomTopic();

            TopicPartition tp0 = new TopicPartition(topic, 0);
            TopicPartition tp1 = new TopicPartition(topic, 1);
            String[] cgcArgs = buildArgsForGroup(cluster, group, "--all-topics", "--to-offset", "2", "--export");
            File file = TestUtils.tempFile("reset", ".csv");

            try (Admin admin = cluster.admin();
                 ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)) {

                admin.createTopics(Set.of(new NewTopic(topic, 2, (short) 1))).all().get();
                produceConsumeAndShutdown(cluster, topic, group, 2, groupProtocol);

                Map<String, Map<TopicPartition, OffsetAndMetadata>> exportedOffsets = service.resetOffsets();

                writeContentToFile(file, service.exportOffsetsToCsv(exportedOffsets));

                Map<TopicPartition, Long> exp1 = new HashMap<>();
                exp1.put(tp0, 2L);
                exp1.put(tp1, 2L);
                assertEquals(exp1, toOffsetMap(exportedOffsets.get(group)));

                String[] cgcArgsExec = buildArgsForGroup(cluster, group, "--all-topics",
                        "--from-file", file.getCanonicalPath(), "--dry-run");
                try (ConsumerGroupCommand.ConsumerGroupService serviceExec = getConsumerGroupService(cgcArgsExec)) {
                    Map<String, Map<TopicPartition, OffsetAndMetadata>> importedOffsets = serviceExec.resetOffsets();
                    assertEquals(exp1, toOffsetMap(importedOffsets.get(group)));
                }

                admin.deleteTopics(Set.of(topic));
            }
        }
    }

    @ClusterTest
    // This one deals with universal CSV export/import file format "group,topic,partition,offset",
    // supporting multiple --group args or --all-groups arg
    public void testResetOffsetsExportImportPlan(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String group1 = generateRandomGroupId();
            String group2 = generateRandomGroupId();
            String topic1 = generateRandomTopic();
            String topic2 = generateRandomTopic();

            TopicPartition t1p0 = new TopicPartition(topic1, 0);
            TopicPartition t1p1 = new TopicPartition(topic1, 1);
            TopicPartition t2p0 = new TopicPartition(topic2, 0);
            TopicPartition t2p1 = new TopicPartition(topic2, 1);
            String[] cgcArgs = buildArgsForGroups(cluster, List.of(group1, group2),
                "--all-topics", "--to-offset", "2", "--export");
            File file = TestUtils.tempFile("reset", ".csv");

            try (Admin admin = cluster.admin();
                 ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)) {

                admin.createTopics(List.of(new NewTopic(topic1, 2, (short) 1),
                        new NewTopic(topic2, 2, (short) 1))).all().get();

                produceConsumeAndShutdown(cluster, topic1, group1, 1, groupProtocol);
                produceConsumeAndShutdown(cluster, topic2, group2, 1, groupProtocol);

                awaitConsumerGroupInactive(service, group1);
                awaitConsumerGroupInactive(service, group2);

                Map<String, Map<TopicPartition, OffsetAndMetadata>> exportedOffsets = service.resetOffsets();

                writeContentToFile(file, service.exportOffsetsToCsv(exportedOffsets));

                Map<TopicPartition, Long> exp1 = new HashMap<>();
                exp1.put(t1p0, 2L);
                exp1.put(t1p1, 2L);
                Map<TopicPartition, Long> exp2 = new HashMap<>();
                exp2.put(t2p0, 2L);
                exp2.put(t2p1, 2L);

                assertEquals(exp1, toOffsetMap(exportedOffsets.get(group1)));
                assertEquals(exp2, toOffsetMap(exportedOffsets.get(group2)));

                // Multiple --group's offset import
                String[] cgcArgsExec = buildArgsForGroups(cluster, List.of(group1, group2),
                        "--all-topics",
                        "--from-file", file.getCanonicalPath(), "--dry-run");
                try (ConsumerGroupCommand.ConsumerGroupService serviceExec = getConsumerGroupService(cgcArgsExec)) {
                    Map<String, Map<TopicPartition, OffsetAndMetadata>> importedOffsets = serviceExec.resetOffsets();
                    assertEquals(exp1, toOffsetMap(importedOffsets.get(group1)));
                    assertEquals(exp2, toOffsetMap(importedOffsets.get(group2)));
                }

                // Single --group offset import using "group,topic,partition,offset" csv format
                String[] cgcArgsExec2 = buildArgsForGroup(cluster, group1, "--all-topics",
                        "--from-file", file.getCanonicalPath(), "--dry-run");
                try (ConsumerGroupCommand.ConsumerGroupService serviceExec2 = getConsumerGroupService(cgcArgsExec2)) {
                    Map<String, Map<TopicPartition, OffsetAndMetadata>> importedOffsets2 = serviceExec2.resetOffsets();
                    assertEquals(exp1, toOffsetMap(importedOffsets2.get(group1)));
                }

                admin.deleteTopics(List.of(topic1, topic2));
            }
        }
    }

    @ClusterTest
    public void testResetWithUnrecognizedNewConsumerOption(ClusterInstance cluster) {
        String group = generateRandomGroupId();
        String[] cgcArgs = new String[]{"--new-consumer",
            "--bootstrap-server", cluster.bootstrapServers(),
            "--reset-offsets", "--group", group, "--all-topics",
            "--to-offset", "2", "--export"};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @ClusterTest(brokers = 3, serverProperties = {@ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2")})
    public void testResetOffsetsWithPartitionNoneLeader(ClusterInstance cluster) throws Exception {
        String group = generateRandomGroupId();
        String topic = generateRandomTopic();
        String[] args = buildArgsForGroup(cluster, group, "--topic", topic + ":0,1,2",
                "--to-earliest", "--execute");

        try (Admin admin = cluster.admin();
             ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(args)) {

            admin.createTopics(Set.of(new NewTopic(topic, 3, (short) 1))).all().get();
            produceConsumeAndShutdown(cluster, topic, group, 2, GroupProtocol.CLASSIC);
            assertDoesNotThrow(() -> resetOffsets(service));
            // shutdown a broker to make some partitions missing leader
            cluster.shutdownBroker(0);
            assertThrows(LeaderNotAvailableException.class, () -> resetOffsets(service));
        }
    }

    @ClusterTest
    public void testResetOffsetsWithPartitionNotExist(ClusterInstance cluster) throws Exception {
        String group = generateRandomGroupId();
        String topic = generateRandomTopic();
        String[] args = buildArgsForGroup(cluster, group, "--topic", topic + ":2,3",
                "--to-earliest", "--execute");

        try (Admin admin = cluster.admin();
             ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(args)) {

            admin.createTopics(Set.of(new NewTopic(topic, 1, (short) 1))).all().get();
            produceConsumeAndShutdown(cluster, topic, group, 2, GroupProtocol.CLASSIC);
            assertThrows(UnknownTopicOrPartitionException.class, () -> resetOffsets(service));
        }
    }

    private String generateRandomTopic() {
        return TOPIC_PREFIX + TestUtils.randomString(10);
    }

    private String generateRandomGroupId() {
        return GROUP_PREFIX + TestUtils.randomString(10);
    }

    private Map<TopicPartition, Long> committedOffsets(ClusterInstance cluster,
                                                       String topic,
                                                       String group) {
        try (Admin admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()))) {
            return admin.listConsumerGroupOffsets(group)
                    .all().get()
                    .get(group).entrySet()
                    .stream()
                    .filter(e -> e.getKey().topic().equals(topic))
                    .collect(toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        return new ConsumerGroupCommand.ConsumerGroupService(
                ConsumerGroupCommandOptions.fromArgs(args),
                Map.of(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)));
    }

    private void produceMessages(ClusterInstance cluster, String topic, int numMessages) {
        List<ProducerRecord<byte[], byte[]>> records = IntStream.range(0, numMessages)
                .mapToObj(i -> new ProducerRecord<byte[], byte[]>(topic, new byte[100 * 1000]))
                .toList();
        produceMessages(cluster, records);
    }

    private void produceMessages(ClusterInstance cluster, List<ProducerRecord<byte[], byte[]>> records) {
        try (Producer<byte[], byte[]> producer = createProducer(cluster)) {
            records.forEach(producer::send);
        }
    }

    private Producer<byte[], byte[]> createProducer(ClusterInstance cluster) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ACKS_CONFIG, "1");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private void resetAndAssertOffsets(ClusterInstance cluster,
                                       String topic,
                                       String[] args,
                                       long expectedOffset) {
        resetAndAssertOffsets(cluster, args, expectedOffset, false, List.of(topic));
    }

    private void resetAndAssertOffsets(ClusterInstance cluster,
                                       String[] args,
                                       long expectedOffset,
                                       boolean dryRun,
                                       List<String> topics) {
        try (ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(args)) {
            Map<String, Map<TopicPartition, Long>> topicToExpectedOffsets = getTopicExceptOffsets(topics, expectedOffset);
            Map<String, Map<TopicPartition, OffsetAndMetadata>> resetOffsetsResultByGroup =
                    resetOffsets(service);
            for (final String topic : topics) {
                resetOffsetsResultByGroup.forEach((group, partitionInfo) -> {
                    Map<TopicPartition, Long> priorOffsets = committedOffsets(cluster, topic, group);
                    assertEquals(topicToExpectedOffsets.get(topic), partitionToOffsets(topic, partitionInfo));
                    assertEquals(dryRun ? priorOffsets : topicToExpectedOffsets.get(topic),
                            committedOffsets(cluster, topic, group));
                });
            }
        }
    }

    private Map<String, Map<TopicPartition, Long>> getTopicExceptOffsets(List<String> topics,
                                                                         long expectedOffset) {
        return topics.stream()
                .collect(toMap(Function.identity(),
                        topic -> Map.of(new TopicPartition(topic, 0),
                                expectedOffset)));
    }

    private Map<String, Map<TopicPartition, OffsetAndMetadata>> resetOffsets(
            ConsumerGroupCommand.ConsumerGroupService consumerGroupService) {
        return consumerGroupService.resetOffsets();
    }

    private Map<TopicPartition, Long> partitionToOffsets(String topic,
                                                         Map<TopicPartition, OffsetAndMetadata> partitionInfo) {
        return partitionInfo.entrySet()
                .stream()
                .filter(entry -> Objects.equals(entry.getKey().topic(), topic))
                .collect(toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private static List<String> generateIds(String name) {
        return IntStream.rangeClosed(1, 2)
                .mapToObj(id -> name + id)
                .toList();
    }

    private void produceConsumeAndShutdown(ClusterInstance cluster,
                                           String topic,
                                           String group,
                                           int numConsumers,
                                           GroupProtocol groupProtocol) throws Exception {
        produceMessages(cluster, topic, 100);
        try (AutoCloseable consumerGroupCloseable =
                     consumerGroupClosable(cluster, numConsumers, topic, group, groupProtocol)) {
            awaitConsumerProgress(cluster, topic, group, 100);
        }
    }

    private void writeContentToFile(File file, String content) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            bw.write(content);
        }
    }

    private AutoCloseable consumerGroupClosable(ClusterInstance cluster,
                                                int numConsumers,
                                                String topic,
                                                String group,
                                                GroupProtocol groupProtocol) {
        Map<String, Object> configs = composeConsumerConfigs(cluster, group, groupProtocol);
        return ConsumerGroupCommandTestUtils.buildConsumers(
                numConsumers,
                false,
                topic,
                () -> new KafkaConsumer<String, String>(configs));
    }

    private Map<String, Object> composeConsumerConfigs(ClusterInstance cluster,
                                                       String group,
                                                       GroupProtocol groupProtocol) {
        HashMap<String, Object> configs = new HashMap<>();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        configs.put(GROUP_ID_CONFIG, group);
        configs.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name);
        configs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        configs.put(GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 1000);
        if (GroupProtocol.CLASSIC == groupProtocol) {
            configs.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        }
        return configs;
    }

    private void awaitConsumerProgress(ClusterInstance cluster,
                                       String topic,
                                       String group,
                                       long count) throws Exception {
        try (Admin admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()))) {
            Supplier<Long> offsets = () -> {
                try {
                    return admin.listConsumerGroupOffsets(group)
                            .all().get().get(group)
                            .entrySet()
                            .stream()
                            .filter(e -> e.getKey().topic().equals(topic))
                            .mapToLong(e -> e.getValue().offset())
                            .sum();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            };
            TestUtils.waitForCondition(() -> offsets.get() == count,
                    "Expected that consumer group has consumed all messages from topic/partition. " +
                            "Expected offset: " + count +
                            ". Actual offset: " + offsets.get());
        }
    }

    private void awaitConsumerGroupInactive(ConsumerGroupCommand.ConsumerGroupService service,
                                            String group) throws Exception {
        TestUtils.waitForCondition(() -> {
            GroupState state = service.collectGroupState(group).groupState();
            return Objects.equals(state, GroupState.EMPTY) || Objects.equals(state, GroupState.DEAD);
        }, "Expected that consumer group is inactive. Actual state: " +
            service.collectGroupState(group).groupState());
    }

    private void resetAndAssertOffsetsCommitted(ClusterInstance cluster,
                                                ConsumerGroupCommand.ConsumerGroupService service,
                                                Map<TopicPartition, Long> expectedOffsets,
                                                String topic) {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> allResetOffsets = resetOffsets(service);

        allResetOffsets.forEach((group, offsetsInfo) -> offsetsInfo.forEach((tp, offsetMetadata) -> {
            assertEquals(expectedOffsets.get(tp), offsetMetadata.offset());
            assertEquals(expectedOffsets, committedOffsets(cluster, topic, group));
        }));
    }

    private Map<TopicPartition, Long> toOffsetMap(Map<TopicPartition, OffsetAndMetadata> map) {
        return map.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private String[] addTo(String[] args, String... extra) {
        List<String> res = new ArrayList<>(List.of(args));
        res.addAll(List.of(extra));
        return res.toArray(new String[0]);
    }
}
