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

import static java.time.Duration.ofMillis;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

public class KStreamKStreamSelfJoinTest {
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @Test
    public void shouldMatchInnerJoinWithSelfJoinWithSingleStream() {
        // Given:
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final StreamsBuilder builder1 = new StreamsBuilder();
        final StreamsBuilder builder2 = new StreamsBuilder();
        final MockApiProcessorSupplier<String, String, Void, Void> supplier1 =
            new MockApiProcessorSupplier<>();
        final MockApiProcessorSupplier<String, String, Void, Void> supplier2 =
            new MockApiProcessorSupplier<>();
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;
        final KStream<String, String> stream1 = builder1.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = builder2.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> selfJoin = stream1.join(
            stream1,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        selfJoin.process(supplier1);

        final KStream<String, String> innerJoin = stream2.join(
            stream2,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        innerJoin.process(supplier2);
        final List<KeyValueTimestamp<String, String>> expected;

        final Topology topology2 =  builder2.build();
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology2)) {
            System.out.println("-------->" + topology2.describe().toString());
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                supplier2.theCapturedProcessor();
            inputTopic.pipeInput("A", "1", 1L);
            inputTopic.pipeInput("B", "1", 2L);
            inputTopic.pipeInput("A", "2", 3L);
            inputTopic.pipeInput("B", "2", 4L);
            inputTopic.pipeInput("B", "3", 5L);
            expected = processor.processed();
        }

        // When:
        final Topology topology1 =  builder1.build(props);
        System.out.println("-------->" + topology1.describe().toString());
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology1, props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                supplier1.theCapturedProcessor();
            inputTopic.pipeInput("A", "1", 1L);
            inputTopic.pipeInput("B", "1", 2L);
            inputTopic.pipeInput("A", "2", 3L);
            inputTopic.pipeInput("B", "2", 4L);
            inputTopic.pipeInput("B", "3", 5L);

            // Then:
            processor.checkAndClearProcessResult(expected.toArray(new KeyValueTimestamp[0]));
        }
    }


    @Test
    public void shouldMatchInnerJoinWithSelfJoinWithTwoStreams() {
        // Given:
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final StreamsBuilder builder1 = new StreamsBuilder();
        final StreamsBuilder builder2 = new StreamsBuilder();
        final MockApiProcessorSupplier<String, String, Void, Void> supplier1 =
            new MockApiProcessorSupplier<>();
        final MockApiProcessorSupplier<String, String, Void, Void> supplier2 =
            new MockApiProcessorSupplier<>();
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;
        final KStream<String, String> stream1 = builder1.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = builder1.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream3 = builder2.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream4 = builder2.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> selfJoin = stream1.join(
            stream2,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        selfJoin.process(supplier1);

        final KStream<String, String> innerJoin = stream3.join(
            stream4,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        innerJoin.process(supplier2);
        final List<KeyValueTimestamp<String, String>> expected;

        final Topology topology2 =  builder2.build();
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology2)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                supplier2.theCapturedProcessor();
            inputTopic.pipeInput("A", "1", 1L);
            inputTopic.pipeInput("B", "1", 2L);
            inputTopic.pipeInput("A", "2", 3L);
            inputTopic.pipeInput("B", "2", 4L);
            inputTopic.pipeInput("B", "3", 5L);
            expected = processor.processed();
        }

        // When:
        final Topology topology1 =  builder1.build(props);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology1, props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                supplier1.theCapturedProcessor();
            inputTopic.pipeInput("A", "1", 1L);
            inputTopic.pipeInput("B", "1", 2L);
            inputTopic.pipeInput("A", "2", 3L);
            inputTopic.pipeInput("B", "2", 4L);
            inputTopic.pipeInput("B", "3", 5L);

            // Then:
            processor.checkAndClearProcessResult(expected.toArray(new KeyValueTimestamp[0]));
        }
    }
}
