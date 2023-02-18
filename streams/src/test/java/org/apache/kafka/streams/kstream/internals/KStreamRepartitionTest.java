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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Optional;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KStreamRepartitionTest {
    private final String inputTopic = "input-topic";

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    private StreamsBuilder builder;

    @Before
    public void setUp() {
        builder = new StreamsBuilder();
    }

    @Test
    public void shouldInvokePartitionerWhenSet() {
        final int[] expectedKeys = new int[]{0, 1};
        @SuppressWarnings("unchecked")
        final StreamPartitioner<Integer, String> streamPartitionerMock = mock(StreamPartitioner.class);

        when(streamPartitionerMock.partitions(anyString(), eq(0), eq("X0"), anyInt())).thenReturn(Optional.of(Collections.singleton(1)));
        when(streamPartitionerMock.partitions(anyString(), eq(1), eq("X1"), anyInt())).thenReturn(Optional.of(Collections.singleton(1)));

        final String repartitionOperationName = "test";
        final Repartitioned<Integer, String> repartitioned = Repartitioned
            .streamPartitioner(streamPartitionerMock)
            .withName(repartitionOperationName);

        builder.<Integer, String>stream(inputTopic)
            .repartition(repartitioned);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<Integer, String> testInputTopic = driver.createInputTopic(inputTopic,
                                                                                           new IntegerSerializer(),
                                                                                           new StringSerializer());

            final String topicName = repartitionOutputTopic(props, repartitionOperationName);

            final TestOutputTopic<Integer, String> testOutputTopic = driver.createOutputTopic(
                topicName,
                new IntegerDeserializer(),
                new StringDeserializer()
            );

            for (int i = 0; i < 2; i++) {
                testInputTopic.pipeInput(expectedKeys[i], "X" + expectedKeys[i], i + 10);
            }

            assertThat(testOutputTopic.readRecord(), equalTo(new TestRecord<>(0, "X0", Instant.ofEpochMilli(10))));
            assertThat(testOutputTopic.readRecord(), equalTo(new TestRecord<>(1, "X1", Instant.ofEpochMilli(11))));
            assertTrue(testOutputTopic.readRecordsToList().isEmpty());
        }

        verify(streamPartitionerMock).partitions(anyString(), eq(0), eq("X0"), anyInt());
        verify(streamPartitionerMock).partitions(anyString(), eq(1), eq("X1"), anyInt());
    }

    @Test
    public void shouldThrowAnExceptionWhenNumberOfPartitionsOfRepartitionOperationsDoNotMatchWhenJoining() {
        final String topicB = "topic-b";
        final String outputTopic = "topic-output";
        final String topicBRepartitionedName = "topic-b-scale-up";
        final String inputTopicRepartitionedName = "input-topic-scale-up";
        final int topicBNumberOfPartitions = 2;
        final int inputTopicNumberOfPartitions = 4;
        final StreamsBuilder builder = new StreamsBuilder();

        final Repartitioned<Integer, String> inputTopicRepartitioned = Repartitioned
                .<Integer, String>as(inputTopicRepartitionedName)
                .withNumberOfPartitions(inputTopicNumberOfPartitions);

        final Repartitioned<Integer, String> topicBRepartitioned = Repartitioned
                .<Integer, String>as(topicBRepartitionedName)
                .withNumberOfPartitions(topicBNumberOfPartitions);

        final KStream<Integer, String> topicBStream = builder
                .stream(topicB, Consumed.with(Serdes.Integer(), Serdes.String()))
                .repartition(topicBRepartitioned);

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
                .repartition(inputTopicRepartitioned)
                .join(topicBStream, (value1, value2) -> value2, JoinWindows.of(Duration.ofSeconds(10)))
                .to(outputTopic);

        final Map<String, Integer> repartitionTopicsWithNumOfPartitions = Utils.mkMap(
                Utils.mkEntry(toRepartitionTopicName(topicBRepartitionedName), topicBNumberOfPartitions),
                Utils.mkEntry(toRepartitionTopicName(inputTopicRepartitionedName), inputTopicNumberOfPartitions)
        );

        final TopologyException expected = assertThrows(
                TopologyException.class, () -> builder.build(props)
        );
        final String expectedErrorMessage = String.format("Following topics do not have the same " +
                        "number of partitions: [%s]",
                new TreeMap<>(repartitionTopicsWithNumOfPartitions));
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains(expectedErrorMessage));
    }

    private String toRepartitionTopicName(final String input) {
        return input + "-repartition";
    }

    private String repartitionOutputTopic(final Properties props, final String repartitionOperationName) {
        return props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-" + repartitionOperationName + "-repartition";
    }
}
