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
import static java.time.Duration.ofSeconds;

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
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;
        final List<KeyValueTimestamp<String, String>> expected;
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Inner join topology
        final MockApiProcessorSupplier<String, String, Void, Void> innerJoinSupplier =
            new MockApiProcessorSupplier<>();
        final KStream<String, String> stream2 = streamsBuilder.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> innerJoin = stream2.join(
            stream2,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        innerJoin.process(innerJoinSupplier);

        final Topology innerJoinTopology =  streamsBuilder.build();
        try (final TopologyTestDriver driver = new TopologyTestDriver(innerJoinTopology)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                innerJoinSupplier.theCapturedProcessor();
            inputTopic.pipeInput("A", "1", 1L);
            inputTopic.pipeInput("B", "1", 2L);
            inputTopic.pipeInput("A", "2", 3L);
            inputTopic.pipeInput("B", "2", 4L);
            inputTopic.pipeInput("B", "3", 5L);
            expected = processor.processed();
        }

        // Self join topology
        final MockApiProcessorSupplier<String, String, Void, Void> selfJoinSupplier =
            new MockApiProcessorSupplier<>();
        final KStream<String, String> stream1 = streamsBuilder.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> selfJoin = stream1.join(
            stream1,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        selfJoin.process(selfJoinSupplier);

        final Topology selfJoinTopology =  streamsBuilder.build(props);
        try (final TopologyTestDriver driver = new TopologyTestDriver(selfJoinTopology, props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                selfJoinSupplier.theCapturedProcessor();
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
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;
        final List<KeyValueTimestamp<String, String>> expected;
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Inner join topology
        final MockApiProcessorSupplier<String, String, Void, Void> innerJoinSupplier =
            new MockApiProcessorSupplier<>();
        final KStream<String, String> stream3 = streamsBuilder.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream4 = streamsBuilder.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> innerJoin = stream3.join(
            stream4,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        innerJoin.process(innerJoinSupplier);

        final Topology innerJoinTopology =  streamsBuilder.build();
        try (final TopologyTestDriver driver = new TopologyTestDriver(innerJoinTopology)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                innerJoinSupplier.theCapturedProcessor();
            inputTopic.pipeInput("A", "1", 1L);
            inputTopic.pipeInput("B", "1", 2L);
            inputTopic.pipeInput("A", "2", 3L);
            inputTopic.pipeInput("B", "2", 4L);
            inputTopic.pipeInput("B", "3", 5L);
            expected = processor.processed();
        }

        // Self join topology
        final MockApiProcessorSupplier<String, String, Void, Void> selfJoinSupplier =
            new MockApiProcessorSupplier<>();
        final KStream<String, String> stream1 = streamsBuilder.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = streamsBuilder.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> selfJoin = stream1.join(
            stream2,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        selfJoin.process(selfJoinSupplier);

        final Topology topology1 =  streamsBuilder.build(props);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology1, props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                selfJoinSupplier.theCapturedProcessor();
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
    public void shouldMatchInnerJoinWithSelfJoinDifferentBeforeAfterWindows() {
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;
        final List<KeyValueTimestamp<String, String>> expected;
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Inner join topology
        final MockApiProcessorSupplier<String, String, Void, Void> innerJoinSupplier =
            new MockApiProcessorSupplier<>();
        final KStream<String, String> stream3 = streamsBuilder.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream4 = streamsBuilder.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> innerJoin = stream3.join(
            stream4,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(11), ofSeconds(10)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        innerJoin.process(innerJoinSupplier);

        final Topology innerJoinTopology =  streamsBuilder.build();
        try (final TopologyTestDriver driver = new TopologyTestDriver(innerJoinTopology)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                innerJoinSupplier.theCapturedProcessor();
            inputTopic.pipeInput("A", "1", 0L);
            inputTopic.pipeInput("A", "2", 11000L);
            inputTopic.pipeInput("B", "1", 12000L);
            inputTopic.pipeInput("A", "3", 13000L);
            inputTopic.pipeInput("A", "4", 15000L);
            inputTopic.pipeInput("C", "1", 16000L);
            inputTopic.pipeInput("D", "1", 17000L);
            inputTopic.pipeInput("A", "5", 30000L);
            expected = processor.processed();
        }

        // Self join topology
        final MockApiProcessorSupplier<String, String, Void, Void> selfJoinSupplier =
            new MockApiProcessorSupplier<>();
        final KStream<String, String> stream1 = streamsBuilder.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = streamsBuilder.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> selfJoin = stream1.join(
            stream2,
            valueJoiner,
            JoinWindows.ofTimeDifferenceAndGrace(ofSeconds(11), ofSeconds(10)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        selfJoin.process(selfJoinSupplier);
        final Topology selfJoinTopology =  streamsBuilder.build(props);
        try (final TopologyTestDriver driver = new TopologyTestDriver(selfJoinTopology, props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                selfJoinSupplier.theCapturedProcessor();
            inputTopic.pipeInput("A", "1", 0L);
            inputTopic.pipeInput("A", "2", 11000L);
            inputTopic.pipeInput("B", "1", 12000L);
            inputTopic.pipeInput("A", "3", 13000L);
            inputTopic.pipeInput("A", "4", 15000L);
            inputTopic.pipeInput("C", "1", 16000L);
            inputTopic.pipeInput("D", "1", 17000L);
            inputTopic.pipeInput("A", "5", 30000L);

            // Then:
            processor.checkAndClearProcessResult(expected.toArray(new KeyValueTimestamp[0]));
        }
    }

    @Test
    public void shouldMatchInnerJoinWithSelfJoinOutOfOrderMessages() {
        props.setProperty(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;
        final List<KeyValueTimestamp<String, String>> expected;
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Inner join topology
        final MockApiProcessorSupplier<String, String, Void, Void> innerJoinSupplier =
            new MockApiProcessorSupplier<>();
        final KStream<String, String> stream3 = streamsBuilder.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream4 = streamsBuilder.stream(
            topic2, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> innerJoin = stream3.join(
            stream4,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofSeconds(10)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        innerJoin.process(innerJoinSupplier);

        final Topology topology2 =  streamsBuilder.build();
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology2)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                innerJoinSupplier.theCapturedProcessor();

            inputTopic.pipeInput("A", "1", 0L);
            inputTopic.pipeInput("A", "2", 9999);
            inputTopic.pipeInput("B", "1", 11000L);
            inputTopic.pipeInput("A", "3", 13000L);
            inputTopic.pipeInput("A", "4", 15000L);
            inputTopic.pipeInput("C", "1", 16000L);
            inputTopic.pipeInput("D", "1", 17000L);
            inputTopic.pipeInput("A", "5", 30000L);
            inputTopic.pipeInput("A", "5", 6000);
            expected = processor.processed();
        }

        // Self join topology
        final KStream<String, String> stream1 = streamsBuilder.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = streamsBuilder.stream(
            topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final MockApiProcessorSupplier<String, String, Void, Void> selfJoinSupplier =
            new MockApiProcessorSupplier<>();
        final KStream<String, String> selfJoin = stream1.join(
            stream2,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofSeconds(10)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        selfJoin.process(selfJoinSupplier);

        final Topology selfJoinTopology =  streamsBuilder.build(props);
        try (final TopologyTestDriver driver = new TopologyTestDriver(selfJoinTopology, props)) {

            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer());
            final MockApiProcessor<String, String, Void, Void> processor =
                selfJoinSupplier.theCapturedProcessor();
            inputTopic.pipeInput("A", "1", 0L);
            inputTopic.pipeInput("A", "2", 9999);
            inputTopic.pipeInput("B", "1", 11000L);
            inputTopic.pipeInput("A", "3", 13000L);
            inputTopic.pipeInput("A", "4", 15000L);
            inputTopic.pipeInput("C", "1", 16000L);
            inputTopic.pipeInput("D", "1", 17000L);
            inputTopic.pipeInput("A", "5", 30000L);
            inputTopic.pipeInput("A", "5", 6000);

            // Then:
            processor.checkAndClearProcessResult(expected.toArray(new KeyValueTimestamp[0]));
        }
    }
}
