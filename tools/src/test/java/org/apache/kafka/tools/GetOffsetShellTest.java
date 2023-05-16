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
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(clusterType = Type.ZK)
@Tag("integration")
public class GetOffsetShellTest {
    private final int topicCount = 4;
    private final int offsetTopicPartitionCount = 4;
    private final ClusterInstance cluster;

    public GetOffsetShellTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    private String getTopicName(int i) {
        return "topic" + i;
    }

    @BeforeEach
    public void setUp() {
    }

    static class Row {
        private String name;
        private int partition;
        private Long timestamp;

        public Row(String name, int partition, Long timestamp) {
            this.name = name;
            this.partition = partition;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;

            if (!(o instanceof Row)) return false;

            Row r = (Row) o;

            return name.equals(r.name) && partition == r.partition && Objects.equals(timestamp, r.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, partition, timestamp);
        }
    }

    @ClusterTest
    public void testNoFilterOptions() {
        try(Admin admin = Admin.create(cluster.config().adminClientProperties())) {
            List<NewTopic> topics = new ArrayList<>();

            IntStream.range(0, topicCount + 1).forEach(i -> topics.add(new NewTopic(getTopicName(i), i, (short)1)));

            admin.createTopics(topics);
        }

        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.config().producerProperties().get("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            IntStream.range(0, topicCount + 1)
                     .forEach(i -> IntStream.range(0, i * i)
                            .forEach(msgCount -> producer.send(
                                    new ProducerRecord<>(getTopicName(i), msgCount % i, null, "val" + msgCount)))
                     );
        }

        String out = ToolsTestUtils.captureStandardOut(() -> GetOffsetShell.mainNoExit(addBootstrapServer()));

        List<Row> parsedStdOut = Arrays.stream(out.split(System.lineSeparator()))
                .map(i -> i.split(":"))
                .filter(i -> i.length >= 2)
                .map(line -> new Row(line[0], Integer.parseInt(line[1]), (line.length == 2 || line[2].isEmpty()) ? null : Long.parseLong(line[2])))
                .collect(Collectors.toList());

        assertEquals(expectedOffsetsWithInternal(), parsedStdOut);
    }

    private List<Row> expectedOffsetsWithInternal() {
        List<Row> consOffsets = IntStream.range(0, offsetTopicPartitionCount + 1)
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

    private String[] addBootstrapServer(String... args) {
        ArrayList<String> newArgs = new ArrayList<>(Arrays.asList(args));
        newArgs.add("--bootstrap-server");
        newArgs.add(cluster.bootstrapServers());

        return newArgs.toArray(new String[0]);
    }
}
