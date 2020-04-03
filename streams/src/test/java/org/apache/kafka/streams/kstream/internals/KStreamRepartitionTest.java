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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.StreamsTestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Instant;
import java.util.Properties;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
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
        final StreamPartitioner<Integer, String> streamPartitionerMock = EasyMock.mock(StreamPartitioner.class);

        expect(streamPartitionerMock.partition(anyString(), eq(0), eq("X0"), anyInt())).andReturn(1).times(1);
        expect(streamPartitionerMock.partition(anyString(), eq(1), eq("X1"), anyInt())).andReturn(1).times(1);
        replay(streamPartitionerMock);

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

        verify(streamPartitionerMock);
    }

    private String repartitionOutputTopic(final Properties props, final String repartitionOperationName) {
        return props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-" + repartitionOperationName + "-repartition";
    }
}
