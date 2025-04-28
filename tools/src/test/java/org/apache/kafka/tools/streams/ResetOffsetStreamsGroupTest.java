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
package org.apache.kafka.tools.streams;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.apache.kafka.tools.consumer.group.ConsumerGroupCommand;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class ResetOffsetStreamsGroupTest {
    private static final String TOPIC_PREFIX = "foo-";

    public static EmbeddedKafkaCluster cluster = null;
    static KafkaStreams streams;
    private static final String APP_ID = "streams-group-command-test";
    private static final String INPUT_TOPIC = "customInputTopic";
    private static final String OUTPUT_TOPIC = "customOutputTopic";

    @BeforeAll
    public static void setup() throws Exception {
        // start the cluster and create the input topic
        final Properties props = new Properties();
        props.setProperty(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams");
        cluster = new EmbeddedKafkaCluster(1, props);
        cluster.start();
        cluster.createTopic(INPUT_TOPIC, 2, 1);


        // start kafka streams
        Properties streamsProp = new Properties();
        streamsProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsProp.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        streamsProp.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsProp.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));

        streams = new KafkaStreams(topology(), streamsProp);
        startApplicationAndWaitUntilRunning(streams);
    }

    @AfterAll
    public static void closeCluster() {
        streams.close();
        cluster.stop();
        cluster = null;
    }

    @Test
    public void testResetOffsetsExistingTopic() {
        String topic = APP_ID;
        String[] args = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--reset-offsets", "--topic", topic, "--to-offset", "50"};
        produceMessages(100);
        resetAndAssertOffsets(args, 50, true, List.of(topic));
//        resetAndAssertOffsets(addTo(args, "--dry-run"),
//            50, true, singletonList(topic));
//        resetAndAssertOffsets(addTo(args, "--execute"),
//            50, false, singletonList(topic));
    }


//    private void resetAndAssertOffsets(String topic,
//                                       String[] args,
//                                       long expectedOffset) {
//        resetAndAssertOffsets(args, expectedOffset, false, singletonList(topic));
//    }

    private void resetAndAssertOffsets(String[] args,
                                       long expectedOffset,
                                       boolean dryRun,
                                       List<String> topics) {
        try (StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args)) {
            Map<String, Map<TopicPartition, Long>> topicToExpectedOffsets = getTopicExceptOffsets(topics, expectedOffset);
            Map<String, Map<TopicPartition, OffsetAndMetadata>> resetOffsetsResultByGroup =
                resetOffsets(service);
            for (final String topic : topics) {
                resetOffsetsResultByGroup.forEach((group, partitionInfo) -> {
                    Map<TopicPartition, Long> priorOffsets = committedOffsets(topic, group);
                    assertEquals(topicToExpectedOffsets.get(topic), partitionToOffsets(topic, partitionInfo));
                    assertEquals(dryRun ? priorOffsets : topicToExpectedOffsets.get(topic),
                        committedOffsets(topic, group));
                });
            }
        }
    }

    private Map<TopicPartition, Long> committedOffsets(String topic,
                                                       String group) {
        try (Admin admin = Admin.create(singletonMap(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()))) {
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

    private Map<TopicPartition, Long> partitionToOffsets(String topic,
                                                         Map<TopicPartition, OffsetAndMetadata> partitionInfo) {
        return partitionInfo.entrySet()
            .stream()
            .filter(entry -> Objects.equals(entry.getKey().topic(), topic))
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }


    private Map<String, Map<TopicPartition, Long>> getTopicExceptOffsets(List<String> topics,
                                                                         long expectedOffset) {
        return topics.stream()
            .collect(toMap(Function.identity(),
                topic -> singletonMap(new TopicPartition(topic, 0),
                    expectedOffset)));
    }

    private Map<String, Map<TopicPartition, OffsetAndMetadata>> resetOffsets(
        StreamsGroupCommand.StreamsGroupService service) {
        return service.resetOffsets();
    }

    private void produceMessages(int numMessages) {
        final List<KeyValue<Long, Long>> data = prepareData(0L, numMessages, 0L);

        IntegrationTestUtils.produceKeyValuesSynchronously(
            INPUT_TOPIC,
            data,
            TestUtils.producerConfig(cluster.bootstrapServers(), LongSerializer.class, LongSerializer.class),
            cluster.time
        );
    }

    private List<KeyValue<Long, Long>> prepareData(final long fromInclusive,
                                                   final long toExclusive,
                                                   final Long... keys) {
        final long dataSize = keys.length * (toExclusive - fromInclusive);
        final List<KeyValue<Long, Long>> data = new ArrayList<>((int) dataSize);

        for (final Long k : keys) {
            for (long v = fromInclusive; v < toExclusive; ++v) {
                data.add(new KeyValue<>(k, v));
            }
        }

        return data;
    }



    private static Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()
            .toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    private StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = StreamsGroupCommandOptions.fromArgs(args);
        return new StreamsGroupCommand.StreamsGroupService(
            opts,
            Map.of(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private static void validateDescribeOutput(
        List<String> args,
        List<String> expectedHeader,
        Set<List<String>> expectedRows,
        List<Integer> dontCareIndices
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> StreamsGroupCommand.main(args.toArray(new String[0])));
            out.set(output);

            String[] lines = output.split("\n");
            if (lines.length == 1 && lines[0].isEmpty()) lines = new String[]{};

            if (lines.length == 0) return false;
            List<String> header = Arrays.asList(lines[0].split("\\s+"));
            if (!expectedHeader.equals(header)) return false;

            Set<List<String>> groupDesc = Arrays.stream(Arrays.copyOfRange(lines, 1, lines.length))
                .map(line -> Arrays.asList(line.split("\\s+")))
                .collect(Collectors.toSet());
            if (groupDesc.size() != expectedRows.size()) return false;
            // clear the dontCare fields and then compare two sets
            return expectedRows
                .equals(
                    groupDesc.stream()
                        .map(list -> {
                            List<String> listCloned = new ArrayList<>(list);
                            dontCareIndices.forEach(index -> listCloned.set(index, ""));
                            return listCloned;
                        }).collect(Collectors.toSet())
                );
        }, () -> String.format("Expected header=%s and groups=%s, but found:%n%s", expectedHeader, expectedRows, out.get()));
    }
}
