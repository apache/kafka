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

import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Exit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(serverProperties = {
    @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
    @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "4")
})
@Tag("integration")
public class GetOffsetShellTest {
    private final int topicCount = 4;
    private final ClusterInstance cluster;

    public GetOffsetShellTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    private String getTopicName(int i) {
        return "topic" + i;
    }

    private void setUp() {
        try (Admin admin = cluster.createAdminClient()) {
            List<NewTopic> topics = new ArrayList<>();

            IntStream.range(0, topicCount + 1).forEach(i -> topics.add(new NewTopic(getTopicName(i), i, (short) 1)));

            admin.createTopics(topics);
        }

        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            IntStream.range(0, topicCount + 1)
                .forEach(i -> IntStream.range(0, i * i)
                        .forEach(msgCount -> {
                            assertDoesNotThrow(() -> producer.send(
                                    new ProducerRecord<>(getTopicName(i), msgCount % i, null, "val" + msgCount)).get());
                        })
                );
        }
    }

    private void createConsumerAndPoll() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<String> topics = new ArrayList<>();
            for (int i = 0; i < topicCount + 1; i++) {
                topics.add(getTopicName(i));
            }
            consumer.subscribe(topics);
            consumer.poll(Duration.ofMillis(1000));
        }
    }

    static class Row {
        private final String name;
        private final int partition;
        private final Long offset;

        public Row(String name, int partition, Long offset) {
            this.name = name;
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "Row[name:" + name + ",partition:" + partition + ",offset:" + offset;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;

            if (!(o instanceof Row)) return false;

            Row r = (Row) o;

            return name.equals(r.name) && partition == r.partition && Objects.equals(offset, r.offset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, partition, offset);
        }
    }

    @ClusterTest
    public void testNoFilterOptions() {
        setUp();

        List<Row> output = executeAndParse();
        if (!cluster.isKRaftTest()) {
            assertEquals(expectedOffsetsWithInternal(), output);
        } else {
            assertEquals(expectedTestTopicOffsets(), output);
        }
    }

    @ClusterTest
    public void testInternalExcluded() {
        setUp();

        List<Row> output = executeAndParse("--exclude-internal-topics");

        assertEquals(expectedTestTopicOffsets(), output);
    }

    @ClusterTest
    public void testTopicNameArg() {
        setUp();

        IntStream.range(1, topicCount + 1).forEach(i -> {
            List<Row> offsets = executeAndParse("--topic", getTopicName(i));

            assertEquals(expectedOffsetsForTopic(i), offsets, () -> "Offset output did not match for " + getTopicName(i));
            }
        );
    }

    @ClusterTest
    public void testTopicPatternArg() {
        setUp();

        List<Row> offsets = executeAndParse("--topic", "topic.*");

        assertEquals(expectedTestTopicOffsets(), offsets);
    }

    @ClusterTest
    public void testPartitionsArg() {
        setUp();

        List<Row> offsets = executeAndParse("--partitions", "0,1");
        if (!cluster.isKRaftTest()) {
            assertEquals(expectedOffsetsWithInternal().stream().filter(r -> r.partition <= 1).collect(Collectors.toList()), offsets);
        } else {
            assertEquals(expectedTestTopicOffsets().stream().filter(r -> r.partition <= 1).collect(Collectors.toList()), offsets);
        }
    }

    @ClusterTest
    public void testTopicPatternArgWithPartitionsArg() {
        setUp();

        List<Row> offsets = executeAndParse("--topic", "topic.*", "--partitions", "0,1");

        assertEquals(expectedTestTopicOffsets().stream().filter(r -> r.partition <= 1).collect(Collectors.toList()), offsets);
    }

    @ClusterTest
    public void testTopicPartitionsArg() {
        setUp();

        createConsumerAndPoll();

        List<Row> offsets = executeAndParse("--topic-partitions", "topic1:0,topic2:1,topic(3|4):2,__.*:3");
        List<Row> expected = Arrays.asList(
                new Row("__consumer_offsets", 3, 0L),
                new Row("topic1", 0, 1L),
                new Row("topic2", 1, 2L),
                new Row("topic3", 2, 3L),
                new Row("topic4", 2, 4L)
        );

        assertEquals(expected, offsets);
    }

    @ClusterTest
    public void testGetLatestOffsets() {
        setUp();

        for (String time : new String[] {"-1", "latest"}) {
            List<Row> offsets = executeAndParse("--topic-partitions", "topic.*:0", "--time", time);
            List<Row> expected = Arrays.asList(
                    new Row("topic1", 0, 1L),
                    new Row("topic2", 0, 2L),
                    new Row("topic3", 0, 3L),
                    new Row("topic4", 0, 4L)
            );

            assertEquals(expected, offsets);
        }
    }

    @ClusterTest
    public void testGetEarliestOffsets() {
        setUp();

        for (String time : new String[] {"-2", "earliest"}) {
            List<Row> offsets = executeAndParse("--topic-partitions", "topic.*:0", "--time", time);
            List<Row> expected = Arrays.asList(
                    new Row("topic1", 0, 0L),
                    new Row("topic2", 0, 0L),
                    new Row("topic3", 0, 0L),
                    new Row("topic4", 0, 0L)
            );

            assertEquals(expected, offsets);
        }
    }

    @ClusterTest
    public void testGetOffsetsByMaxTimestamp() {
        setUp();

        for (String time : new String[] {"-3", "max-timestamp"}) {
            List<Row> offsets = executeAndParse("--topic-partitions", "topic.*", "--time", time);

            offsets.forEach(
                    row -> assertTrue(row.offset >= 0 && row.offset <= Integer.parseInt(row.name.replace("topic", "")))
            );
        }
    }

    @ClusterTest
    public void testGetOffsetsByTimestamp() {
        setUp();

        String time = String.valueOf(System.currentTimeMillis() / 2);

        List<Row> offsets = executeAndParse("--topic-partitions", "topic.*:0", "--time", time);
        List<Row> expected = Arrays.asList(
                new Row("topic1", 0, 0L),
                new Row("topic2", 0, 0L),
                new Row("topic3", 0, 0L),
                new Row("topic4", 0, 0L)
        );

        assertEquals(expected, offsets);
    }

    @ClusterTest
    public void testNoOffsetIfTimestampGreaterThanLatestRecord() {
        setUp();

        String time = String.valueOf(System.currentTimeMillis() * 2);

        List<Row> offsets = executeAndParse("--topic-partitions", "topic.*", "--time", time);

        assertEquals(new ArrayList<Row>(), offsets);
    }

    @ClusterTest
    public void testTopicPartitionsArgWithInternalExcluded() {
        setUp();

        List<Row> offsets = executeAndParse("--topic-partitions", "topic1:0,topic2:1,topic(3|4):2,__.*:3", "--exclude-internal-topics");
        List<Row> expected = Arrays.asList(
                new Row("topic1", 0, 1L),
                new Row("topic2", 1, 2L),
                new Row("topic3", 2, 3L),
                new Row("topic4", 2, 4L)
        );

        assertEquals(expected, offsets);
    }

    @ClusterTest
    public void testTopicPartitionsArgWithInternalIncluded() {
        setUp();

        createConsumerAndPoll();

        List<Row> offsets = executeAndParse("--topic-partitions", "__.*:0");

        assertEquals(Collections.singletonList(new Row("__consumer_offsets", 0, 0L)), offsets);
    }

    @ClusterTest
    public void testTopicPartitionsNotFoundForNonExistentTopic() {
        assertExitCodeIsOne("--topic", "some_nonexistent_topic");
    }

    @ClusterTest
    public void testTopicPartitionsNotFoundForExcludedInternalTopic() {
        assertExitCodeIsOne("--topic", "some_nonexistent_topic:*");
    }

    @ClusterTest
    public void testTopicPartitionsNotFoundForNonMatchingTopicPartitionPattern() {
        assertExitCodeIsOne("--topic-partitions", "__consumer_offsets", "--exclude-internal-topics");
    }

    @ClusterTest
    public void testTopicPartitionsFlagWithTopicFlagCauseExit() {
        assertExitCodeIsOne("--topic-partitions", "__consumer_offsets", "--topic", "topic1");
    }

    @ClusterTest
    public void testTopicPartitionsFlagWithPartitionsFlagCauseExit() {
        assertExitCodeIsOne("--topic-partitions", "__consumer_offsets", "--partitions", "0");
    }

    @ClusterTest
    public void testPrintHelp() {
        Exit.setExitProcedure((statusCode, message) -> { });
        try {
            String out = ToolsTestUtils.captureStandardErr(() -> GetOffsetShell.mainNoExit("--help"));
            assertTrue(out.startsWith(GetOffsetShell.USAGE_TEXT));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @ClusterTest
    public void testPrintVersion() {
        String out = ToolsTestUtils.captureStandardOut(() -> GetOffsetShell.mainNoExit("--version"));
        assertEquals(AppInfoParser.getVersion(), out);
    }

    private void assertExitCodeIsOne(String... args) {
        final int[] exitStatus = new int[1];

        Exit.setExitProcedure((statusCode, message) -> {
            exitStatus[0] = statusCode;

            throw new RuntimeException();
        });

        try {
            GetOffsetShell.main(addBootstrapServer(args));
        } catch (RuntimeException ignored) {

        } finally {
            Exit.resetExitProcedure();
        }

        assertEquals(1, exitStatus[0]);
    }

    private List<Row> expectedOffsetsWithInternal() {
        List<Row> consOffsets = IntStream.range(0, 4)
                .mapToObj(i -> new Row("__consumer_offsets", i, 0L))
                .collect(Collectors.toList());

        return Stream.concat(consOffsets.stream(), expectedTestTopicOffsets().stream()).collect(Collectors.toList());
    }

    private List<Row> expectedTestTopicOffsets() {
        List<Row> offsets = new ArrayList<>(topicCount + 1);

        for (int i = 0; i < topicCount + 1; i++) {
            offsets.addAll(expectedOffsetsForTopic(i));
        }

        return offsets;
    }

    private List<Row> expectedOffsetsForTopic(int i) {
        String name = getTopicName(i);

        return IntStream.range(0, i).mapToObj(p -> new Row(name, p, (long) i)).collect(Collectors.toList());
    }

    private List<Row> executeAndParse(String... args) {
        String out = ToolsTestUtils.captureStandardOut(() -> GetOffsetShell.mainNoExit(addBootstrapServer(args)));

        return Arrays.stream(out.split(System.lineSeparator()))
                .map(i -> i.split(":"))
                .filter(i -> i.length >= 2)
                .map(line -> new Row(line[0], Integer.parseInt(line[1]), (line.length == 2 || line[2].isEmpty()) ? null : Long.parseLong(line[2])))
                .collect(Collectors.toList());
    }

    private String[] addBootstrapServer(String... args) {
        ArrayList<String> newArgs = new ArrayList<>(Arrays.asList(args));
        newArgs.add("--bootstrap-server");
        newArgs.add(cluster.bootstrapServers());

        return newArgs.toArray(new String[0]);
    }
}
