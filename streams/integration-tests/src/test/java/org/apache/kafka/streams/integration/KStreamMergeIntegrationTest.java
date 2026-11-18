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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
@Timeout(120)
public class KStreamMergeIntegrationTest {
    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private static final String SINGLE_PARTITION_INPUT = "single-partition-input";
    private static final String THREE_PARTITION_INPUT = "three-partition-input";
    private static final String OUTPUT = "merged-output";

    @BeforeAll
    public static void startCluster() {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = {"classic", "streams"})
    public void shouldMergeSourceTopicsWithDifferentPartitionCounts(final String groupProtocol) throws Exception {
        final Set<KeyValue<String, String>> expectedRecords = Set.of(
            KeyValue.pair("single-0", "value-single-0"),
            KeyValue.pair("three-0", "value-three-0"),
            KeyValue.pair("three-1", "value-three-1"),
            KeyValue.pair("three-2", "value-three-2")
        );

        verifyMergeWithDifferentPartitionCounts(groupProtocol, expectedRecords);
    }

    private void verifyMergeWithDifferentPartitionCounts(final String groupProtocol,
                                                         final Set<KeyValue<String, String>> expectedRecords) throws Exception {
        final String singlePartitionInput = SINGLE_PARTITION_INPUT + "-" + groupProtocol;
        final String threePartitionInput = THREE_PARTITION_INPUT + "-" + groupProtocol;
        final String output = OUTPUT + "-" + groupProtocol;
        CLUSTER.createTopic(singlePartitionInput, 1, 1);
        CLUSTER.createTopic(threePartitionInput, 3, 1);
        CLUSTER.createTopic(output, 1, 1);

        final Properties streamsConfig = getStreamsConfig(groupProtocol);
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(singlePartitionInput)
            .merge(builder.stream(threePartitionInput))
            .to(output);

        final Properties producerConfig = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class
        );
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            producer.send(new ProducerRecord<>(singlePartitionInput, 0, "single-0", "value-single-0")).get();
            producer.send(new ProducerRecord<>(threePartitionInput, 0, "three-0", "value-three-0")).get();
            producer.send(new ProducerRecord<>(threePartitionInput, 1, "three-1", "value-three-1")).get();
            producer.send(new ProducerRecord<>(threePartitionInput, 2, "three-2", "value-three-2")).get();
        }

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig)) {
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<String, String>> actualRecords = waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class),
                output,
                expectedRecords.size()
            );
            assertEquals(expectedRecords, new HashSet<>(actualRecords));
        }
    }

    private Properties getStreamsConfig(final String groupProtocol) {
        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "merge-different-partition-counts-" + groupProtocol);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);

        return streamsConfig;
    }
}
