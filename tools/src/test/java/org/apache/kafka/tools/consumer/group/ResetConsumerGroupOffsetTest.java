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

import joptsimple.OptionException;
import kafka.admin.ConsumerGroupCommand;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import scala.Option;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
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
public class ResetConsumerGroupOffsetTest extends ConsumerGroupCommandTest {
    private String[] basicArgs() {
        return new String[]{"--reset-offsets",
            "--bootstrap-server", bootstrapServers(listenerName()),
            "--timeout", Long.toString(DEFAULT_MAX_WAIT_MS)};
    }

    private String[] buildArgsForGroups(List<String> groups, String...args) {
        List<String> res = new ArrayList<>(Arrays.asList(basicArgs()));
        for (String group : groups) {
            res.add("--group");
            res.add(group);
        }
        res.addAll(Arrays.asList(args));
        return res.toArray(new String[0]);
    }

    private String[] buildArgsForGroup(String group, String...args) {
        return buildArgsForGroups(Collections.singletonList(group), args);
    }

    private String[] buildArgsForAllGroups(String...args) {
        List<String> res = new ArrayList<>(Arrays.asList(basicArgs()));
        res.add("--all-groups");
        res.addAll(Arrays.asList(args));
        return res.toArray(new String[0]);
    }

    @Test
    public void testResetOffsetsNotExistingGroup() throws Exception {
        String group = "missing.group";
        String[] args = buildArgsForGroup(group, "--all-topics", "--to-current", "--execute");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);
        // Make sure we got a coordinator
        TestUtils.waitForCondition(
            () -> Objects.equals(consumerGroupCommand.collectGroupState(group).coordinator().host(), "localhost"),
            "Can't find a coordinator");
        Option<scala.collection.Map<TopicPartition, OffsetAndMetadata>> resetOffsets = consumerGroupCommand.resetOffsets().get(group);
        assertTrue(resetOffsets.isDefined() && resetOffsets.get().isEmpty());
        assertTrue(committedOffsets(TOPIC, group).isEmpty());
    }

    @Test
    public void testResetOffsetsExistingTopic() {
        String group = "new.group";
        String[] args = buildArgsForGroup(group, "--topic", TOPIC, "--to-offset", "50");
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 50, true, Collections.singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--dry-run"), 50, true, Collections.singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--execute"), 50, false, Collections.singletonList(TOPIC));
    }

    @Test
    public void testResetOffsetsExistingTopicSelectedGroups() throws Exception {
        produceMessages(TOPIC, 100);
        List<String> groups = IntStream.rangeClosed(1, 3).mapToObj(id -> GROUP + id).collect(Collectors.toList());
        for (String group : groups) {
            ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, group, GroupProtocol.CLASSIC.name);
            awaitConsumerProgress(TOPIC, group, 100L);
            executor.shutdown();
        }
        String[] args = buildArgsForGroups(groups, "--topic", TOPIC, "--to-offset", "50");
        resetAndAssertOffsets(args, 50, true, Collections.singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--dry-run"), 50, true, Collections.singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--execute"), 50, false, Collections.singletonList(TOPIC));
    }

    @Test
    public void testResetOffsetsExistingTopicAllGroups() throws Exception {
        String[] args = buildArgsForAllGroups("--topic", TOPIC, "--to-offset", "50");
        produceMessages(TOPIC, 100);
        for (int i = 1; i <= 3; i++) {
            String group = GROUP + i;
            ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, group, GroupProtocol.CLASSIC.name);
            awaitConsumerProgress(TOPIC, group, 100L);
            executor.shutdown();
        }
        resetAndAssertOffsets(args, 50, true, Collections.singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--dry-run"), 50, true, Collections.singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--execute"), 50, false, Collections.singletonList(TOPIC));
    }

    @Test
    public void testResetOffsetsAllTopicsAllGroups() throws Exception {
        String[] args = buildArgsForAllGroups("--all-topics", "--to-offset", "50");
        List<String> topics = IntStream.rangeClosed(1, 3).mapToObj(i -> TOPIC + i).collect(Collectors.toList());
        List<String> groups = IntStream.rangeClosed(1, 3).mapToObj(i -> GROUP + i).collect(Collectors.toList());
        topics.forEach(topic -> produceMessages(topic, 100));

        for (String topic : topics) {
            for (String group : groups) {
                ConsumerGroupExecutor executor = addConsumerGroupExecutor(3, topic, group, GroupProtocol.CLASSIC.name);
                awaitConsumerProgress(topic, group, 100);
                executor.shutdown();
            }
        }
        resetAndAssertOffsets(args, 50, true, topics);
        resetAndAssertOffsets(addTo(args, "--dry-run"), 50, true, topics);
        resetAndAssertOffsets(addTo(args, "--execute"), 50, false, topics);
    }

    @Test
    public void testResetOffsetsToLocalDateTime() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);

        produceMessages(TOPIC, 100);

        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, GROUP, GroupProtocol.CLASSIC.name);
        awaitConsumerProgress(TOPIC, GROUP, 100L);
        executor.shutdown();

        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-datetime", format.format(calendar.getTime()), "--execute");
        resetAndAssertOffsets(args, 0);
    }

    @Test
    public void testResetOffsetsToZonedDateTime() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

        produceMessages(TOPIC, 50);
        Date checkpoint = new Date();
        produceMessages(TOPIC, 50);

        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, GROUP, GroupProtocol.CLASSIC.name);
        awaitConsumerProgress(TOPIC, GROUP, 100L);
        executor.shutdown();

        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-datetime", format.format(checkpoint), "--execute");
        resetAndAssertOffsets(args, 50);
    }

    @Test
    public void testResetOffsetsByDuration() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--by-duration", "PT1M", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 0);
    }

    @Test
    public void testResetOffsetsByDurationToEarliest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--by-duration", "PT0.1S", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 100);
    }

    @Test
    public void testResetOffsetsByDurationFallbackToLatestWhenNoRecords() throws Exception {
        String topic = "foo2";
        String[] args = buildArgsForGroup(GROUP, "--topic", topic, "--by-duration", "PT1M", "--execute");
        createTopic(topic, 1, 1, new Properties(), listenerName(), new Properties());
        resetAndAssertOffsets(args, 0, false, Collections.singletonList("foo2"));

        adminZkClient().deleteTopic(topic);
    }

    @Test
    public void testResetOffsetsToEarliest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-earliest", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 0);
    }

    @Test
    public void testResetOffsetsToLatest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-latest", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 200);
    }

    @Test
    public void testResetOffsetsToCurrentOffset() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-current", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 100);
    }

    @Test
    public void testResetOffsetsToSpecificOffset() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-offset", "1", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 1);
    }

    @Test
    public void testResetOffsetsShiftPlus() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--shift-by", "50", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 150);
    }

    @Test
    public void testResetOffsetsShiftMinus() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--shift-by", "-50", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 50);
    }

    @Test
    public void testResetOffsetsShiftByLowerThanEarliest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--shift-by", "-150", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 0);
    }

    @Test
    public void testResetOffsetsShiftByHigherThanLatest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--shift-by", "150", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 200);
    }

    @Test
    public void testResetOffsetsToEarliestOnOneTopic() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--topic", TOPIC, "--to-earliest", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 0);
    }

    @Test
    public void testResetOffsetsToEarliestOnOneTopicAndPartition() throws Exception {
        String topic = "bar";
        createTopic(topic, 2, 1, new Properties(), listenerName(), new Properties());

        String[] args = buildArgsForGroup(GROUP, "--topic", topic + ":1", "--to-earliest", "--execute");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);

        produceConsumeAndShutdown(topic, GROUP, 100, 2);
        Map<TopicPartition, Long> priorCommittedOffsets = committedOffsets(topic, GROUP);

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(tp0, priorCommittedOffsets.get(tp0));
        expectedOffsets.put(tp1, 0L);
        resetAndAssertOffsetsCommitted(consumerGroupCommand, expectedOffsets, topic);

        adminZkClient().deleteTopic(topic);
    }

    @Test
    public void testResetOffsetsToEarliestOnTopics() throws Exception {
        String topic1 = "topic1";
        String topic2 = "topic2";
        createTopic(topic1, 1, 1, new Properties(), listenerName(), new Properties());
        createTopic(topic2, 1, 1, new Properties(), listenerName(), new Properties());

        String[] args = buildArgsForGroup(GROUP, "--topic", topic1, "--topic", topic2, "--to-earliest", "--execute");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);

        produceConsumeAndShutdown(topic1, GROUP, 100, 1);
        produceConsumeAndShutdown(topic2, GROUP, 100, 1);

        TopicPartition tp1 = new TopicPartition(topic1, 0);
        TopicPartition tp2 = new TopicPartition(topic2, 0);

        Map<TopicPartition, Long> allResetOffsets = toOffsetMap(consumerGroupCommand.resetOffsets().get(GROUP));
        Map<TopicPartition, Long> expMap = new HashMap<>();
        expMap.put(tp1, 0L);
        expMap.put(tp2, 0L);
        assertEquals(expMap, allResetOffsets);
        assertEquals(Collections.singletonMap(tp1, 0L), committedOffsets(topic1, GROUP));
        assertEquals(Collections.singletonMap(tp2, 0L), committedOffsets(topic2, GROUP));

        adminZkClient().deleteTopic(topic1);
        adminZkClient().deleteTopic(topic2);
    }

    @Test
    public void testResetOffsetsToEarliestOnTopicsAndPartitions() throws Exception {
        String topic1 = "topic1";
        String topic2 = "topic2";

        createTopic(topic1, 2, 1, new Properties(), listenerName(), new Properties());
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        String[] args = buildArgsForGroup(GROUP, "--topic", topic1 + ":1", "--topic", topic2 + ":1", "--to-earliest", "--execute");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);

        produceConsumeAndShutdown(topic1, GROUP, 100, 2);
        produceConsumeAndShutdown(topic2, GROUP, 100, 2);

        Map<TopicPartition, Long> priorCommittedOffsets1 = committedOffsets(topic1, GROUP);
        Map<TopicPartition, Long> priorCommittedOffsets2 = committedOffsets(topic2, GROUP);

        TopicPartition tp1 = new TopicPartition(topic1, 1);
        TopicPartition tp2 = new TopicPartition(topic2, 1);
        Map<TopicPartition, Long> allResetOffsets = toOffsetMap(consumerGroupCommand.resetOffsets().get(GROUP));
        Map<TopicPartition, Long> expMap = new HashMap<>();
        expMap.put(tp1, 0L);
        expMap.put(tp2, 0L);
        assertEquals(expMap, allResetOffsets);
        priorCommittedOffsets1.put(tp1, 0L);
        assertEquals(priorCommittedOffsets1, committedOffsets(topic1, GROUP));
        priorCommittedOffsets2.put(tp2, 0L);
        assertEquals(priorCommittedOffsets2, committedOffsets(topic2, GROUP));

        adminZkClient().deleteTopic(topic1);
        adminZkClient().deleteTopic(topic2);
    }

    @Test
    // This one deals with old CSV export/import format for a single --group arg: "topic,partition,offset" to support old behavior
    public void testResetOffsetsExportImportPlanSingleGroupArg() throws Exception {
        String topic = "bar";
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        createTopic(topic, 2, 1, new Properties(), listenerName(), new Properties());

        String[] cgcArgs = buildArgsForGroup(GROUP, "--all-topics", "--to-offset", "2", "--export");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(cgcArgs);

        produceConsumeAndShutdown(topic, GROUP, 100, 2);

        File file = TestUtils.tempFile("reset", ".csv");

        scala.collection.Map<String, scala.collection.Map<TopicPartition, OffsetAndMetadata>> exportedOffsets = consumerGroupCommand.resetOffsets();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write(consumerGroupCommand.exportOffsetsToCsv(exportedOffsets));
        bw.close();

        Map<TopicPartition, Long> exp1 = new HashMap<>();
        exp1.put(tp0, 2L);
        exp1.put(tp1, 2L);
        assertEquals(exp1, toOffsetMap(exportedOffsets.get(GROUP)));

        String[] cgcArgsExec = buildArgsForGroup(GROUP, "--all-topics", "--from-file", file.getCanonicalPath(), "--dry-run");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommandExec = getConsumerGroupService(cgcArgsExec);
        scala.collection.Map<String, scala.collection.Map<TopicPartition, OffsetAndMetadata>> importedOffsets = consumerGroupCommandExec.resetOffsets();
        assertEquals(exp1, toOffsetMap(importedOffsets.get(GROUP)));

        adminZkClient().deleteTopic(topic);
    }

    @Test
    // This one deals with universal CSV export/import file format "group,topic,partition,offset",
    // supporting multiple --group args or --all-groups arg
    public void testResetOffsetsExportImportPlan() throws Exception {
        String group1 = GROUP + "1";
        String group2 = GROUP + "2";
        String topic1 = "bar1";
        String topic2 = "bar2";
        TopicPartition t1p0 = new TopicPartition(topic1, 0);
        TopicPartition t1p1 = new TopicPartition(topic1, 1);
        TopicPartition t2p0 = new TopicPartition(topic2, 0);
        TopicPartition t2p1 = new TopicPartition(topic2, 1);
        createTopic(topic1, 2, 1, new Properties(), listenerName(), new Properties());
        createTopic(topic2, 2, 1, new Properties(), listenerName(), new Properties());

        String[] cgcArgs = buildArgsForGroups(Arrays.asList(group1, group2), "--all-topics", "--to-offset", "2", "--export");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(cgcArgs);

        produceConsumeAndShutdown(topic1, group1, 100, 1);
        produceConsumeAndShutdown(topic2, group2, 100, 1);

        awaitConsumerGroupInactive(consumerGroupCommand, group1);
        awaitConsumerGroupInactive(consumerGroupCommand, group2);

        File file = TestUtils.tempFile("reset", ".csv");

        scala.collection.Map<String, scala.collection.Map<TopicPartition, OffsetAndMetadata>> exportedOffsets = consumerGroupCommand.resetOffsets();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write(consumerGroupCommand.exportOffsetsToCsv(exportedOffsets));
        bw.close();
        Map<TopicPartition, Long> exp1 = new HashMap<>();
        exp1.put(t1p0, 2L);
        exp1.put(t1p1, 2L);
        Map<TopicPartition, Long> exp2 = new HashMap<>();
        exp2.put(t2p0, 2L);
        exp2.put(t2p1, 2L);

        assertEquals(exp1, toOffsetMap(exportedOffsets.get(group1)));
        assertEquals(exp2, toOffsetMap(exportedOffsets.get(group2)));

        // Multiple --group's offset import
        String[] cgcArgsExec = buildArgsForGroups(Arrays.asList(group1, group2), "--all-topics", "--from-file", file.getCanonicalPath(), "--dry-run");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommandExec = getConsumerGroupService(cgcArgsExec);
        scala.collection.Map<String, scala.collection.Map<TopicPartition, OffsetAndMetadata>> importedOffsets = consumerGroupCommandExec.resetOffsets();
        assertEquals(exp1, toOffsetMap(importedOffsets.get(group1)));
        assertEquals(exp2, toOffsetMap(importedOffsets.get(group2)));

        // Single --group offset import using "group,topic,partition,offset" csv format
        String[] cgcArgsExec2 = buildArgsForGroup(group1, "--all-topics", "--from-file", file.getCanonicalPath(), "--dry-run");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommandExec2 = getConsumerGroupService(cgcArgsExec2);
        scala.collection.Map<String, scala.collection.Map<TopicPartition, OffsetAndMetadata>> importedOffsets2 = consumerGroupCommandExec2.resetOffsets();
        assertEquals(exp1, toOffsetMap(importedOffsets2.get(group1)));

        adminZkClient().deleteTopic(TOPIC);
    }

    @Test
    public void testResetWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", bootstrapServers(listenerName()), "--reset-offsets",
            "--group", GROUP, "--all-topics", "--to-offset", "2", "--export"};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    private void produceMessages(String topic, int numMessages) {
        List<ProducerRecord<byte[], byte[]>> records = IntStream.range(0, numMessages)
            .mapToObj(i -> new ProducerRecord<byte[], byte[]>(topic, new byte[100 * 1000]))
            .collect(Collectors.toList());
        kafka.utils.TestUtils.produceMessages(servers(), seq(records), 1);
    }

    private void produceConsumeAndShutdown(String topic, String group, int totalMessages, int numConsumers) throws Exception {
        produceMessages(topic, totalMessages);
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(numConsumers, topic, group, GroupProtocol.CLASSIC.name);
        awaitConsumerProgress(topic, group, totalMessages);
        executor.shutdown();
    }

    private void awaitConsumerProgress(String topic,
                                       String group,
                                       long count) throws Exception {
        try (Consumer<String, String> consumer = createNoAutoCommitConsumer(group)) {
            Set<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toSet());

            TestUtils.waitForCondition(() -> {
                Collection<OffsetAndMetadata> committed = consumer.committed(partitions).values();
                long total = committed.stream()
                    .mapToLong(offsetAndMetadata -> Optional.ofNullable(offsetAndMetadata).map(OffsetAndMetadata::offset).orElse(0L))
                    .sum();

                return total == count;
            }, "Expected that consumer group has consumed all messages from topic/partition. " +
                "Expected offset: " + count + ". Actual offset: " + committedOffsets(topic, group).values().stream().mapToLong(Long::longValue).sum());
        }
    }

    private void awaitConsumerGroupInactive(ConsumerGroupCommand.ConsumerGroupService consumerGroupService, String group) throws Exception {
        TestUtils.waitForCondition(() -> {
            String state = consumerGroupService.collectGroupState(group).state();
            return Objects.equals(state, "Empty") || Objects.equals(state, "Dead");
        }, "Expected that consumer group is inactive. Actual state: " + consumerGroupService.collectGroupState(group).state());
    }

    private void resetAndAssertOffsets(String[] args,
                                       long expectedOffset) {
        resetAndAssertOffsets(args, expectedOffset, false, Collections.singletonList(TOPIC));
    }

    private void resetAndAssertOffsets(String[] args,
                                      long expectedOffset,
                                      boolean dryRun,
                                      List<String> topics) {
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);
        Map<String, Map<TopicPartition, Long>> expectedOffsets = topics.stream().collect(Collectors.toMap(
            Function.identity(),
            topic -> Collections.singletonMap(new TopicPartition(topic, 0), expectedOffset)));
        scala.collection.Map<String, scala.collection.Map<TopicPartition, OffsetAndMetadata>> resetOffsetsResultByGroup = consumerGroupCommand.resetOffsets();

        try {
            for (final String topic : topics) {
                resetOffsetsResultByGroup.foreach(entry -> {
                    String group = entry._1;
                    scala.collection.Map<TopicPartition, OffsetAndMetadata> partitionInfo = entry._2;
                    Map<TopicPartition, Long> priorOffsets = committedOffsets(topic, group);
                    Map<TopicPartition, Long> offsets = new HashMap<>();
                    partitionInfo.foreach(partitionInfoEntry -> {
                        TopicPartition tp = partitionInfoEntry._1;
                        OffsetAndMetadata offsetAndMetadata = partitionInfoEntry._2;
                        if (Objects.equals(tp.topic(), topic))
                            offsets.put(tp, offsetAndMetadata.offset());
                        return null;
                    });
                    assertEquals(expectedOffsets.get(topic), offsets);
                    assertEquals(dryRun ? priorOffsets : expectedOffsets.get(topic), committedOffsets(topic, group));
                    return null;
                });
            }
        } finally {
            consumerGroupCommand.close();
        }
    }

    private void resetAndAssertOffsetsCommitted(ConsumerGroupCommand.ConsumerGroupService consumerGroupService,
                                                Map<TopicPartition, Long> expectedOffsets,
                                                String topic) {
        scala.collection.Map<String, scala.collection.Map<TopicPartition, OffsetAndMetadata>> allResetOffsets = consumerGroupService.resetOffsets();

        allResetOffsets.foreach(entry -> {
            String group = entry._1;
            scala.collection.Map<TopicPartition, OffsetAndMetadata> offsetsInfo = entry._2;
            offsetsInfo.foreach(offsetInfoEntry -> {
                TopicPartition tp = offsetInfoEntry._1;
                OffsetAndMetadata offsetMetadata = offsetInfoEntry._2;
                assertEquals(offsetMetadata.offset(), expectedOffsets.get(tp));
                assertEquals(expectedOffsets, committedOffsets(topic, group));
                return null;
            });
            return null;
        });
    }

    Map<TopicPartition, Long> toOffsetMap(Option<scala.collection.Map<TopicPartition, OffsetAndMetadata>> map) {
        assertTrue(map.isDefined());
        Map<TopicPartition, Long> res = new HashMap<>();
        map.foreach(m -> {
            m.foreach(entry -> {
                TopicPartition tp = entry._1;
                OffsetAndMetadata offsetAndMetadata = entry._2;
                res.put(tp, offsetAndMetadata.offset());
                return null;
            });
            return null;
        });
        return res;
    }

    private String[] addTo(String[] args, String...extra) {
        List<String> res = new ArrayList<>(Arrays.asList(args));
        res.addAll(Arrays.asList(extra));
        return res.toArray(new String[0]);
    }
}
