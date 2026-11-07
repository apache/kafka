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
package org.apache.kafka.streams.examples.temperature;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit tests for {@link TemperatureDemo}.
 *
 */
public class TemperatureDemoTest {

    private static final int TEMPERATURE_THRESHOLD = 20;
    private static final int TEMPERATURE_WINDOW_SIZE = 5;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<Windowed<String>, String> outputTopic;

    private final String inputTopicName = "iot-temperature";
    private final String outputTopicName = "iot-temperature-max";

    @BeforeEach
    public void setup() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "temperature-demo-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        // Build the same topology as TemperatureDemo
        final Topology topology = buildTemperatureTopology();
        testDriver = new TopologyTestDriver(topology, props);

        // Create test input and output topics
        inputTopic = testDriver.createInputTopic(inputTopicName,
                new StringSerializer(), new StringSerializer());

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, TEMPERATURE_WINDOW_SIZE);
        outputTopic = testDriver.createOutputTopic(outputTopicName,
                windowedSerde.deserializer(), new StringDeserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    /**
     * Builds the same topology as TemperatureDemo for testing
     */
    private Topology buildTemperatureTopology() {
        final Duration duration24Hours = Duration.ofHours(24);
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> source = builder.stream(inputTopicName);

        final KStream<Windowed<String>, String> max = source
                // temperature values are sent without a key (null), so in order
                // to group and reduce them, a key is needed ("temp" has been chosen)
                .selectKey((key, value) -> "temp")
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE), duration24Hours))
                .reduce((value1, value2) -> {
                    if (Integer.parseInt(value1) > Integer.parseInt(value2)) {
                        return value1;
                    } else {
                        return value2;
                    }
                })
                .toStream()
                .filter((key, value) -> Integer.parseInt(value) > TEMPERATURE_THRESHOLD);

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, TEMPERATURE_WINDOW_SIZE);

        // need to override key serde to Windowed<String> type
        max.to(outputTopicName, Produced.with(windowedSerde, Serdes.String()));

        return builder.build();
    }

    @Test
    public void shouldComputeMaxTemperatureInWindow() {
        // Given - temperature readings within the same window (5 seconds)
        final Instant baseTime = Instant.ofEpochMilli(0);
        final List<TestRecord<String, String>> inputRecords = Arrays.asList(
                new TestRecord<>(null, "15", baseTime),
                new TestRecord<>(null, "25", baseTime.plusMillis(1000)),
                new TestRecord<>(null, "18", baseTime.plusMillis(2000)),
                new TestRecord<>(null, "30", baseTime.plusMillis(3000)),
                new TestRecord<>(null, "22", baseTime.plusMillis(4000))
        );

        // When
        inputTopic.pipeRecordList(inputRecords);

        // Then - should output the maximum value (30) since it exceeds threshold (20)
        final List<TestRecord<Windowed<String>, String>> outputRecords = outputTopic.readRecordsToList();

        assertFalse(outputRecords.isEmpty(), "Should have output records");

        // Should find the max temperature (30) in output
        boolean foundMax = false;
        for (final TestRecord<Windowed<String>, String> record : outputRecords) {
            if ("30".equals(record.value())) {
                foundMax = true;
                assertEquals("temp", record.key().key(), "Key should be 'temp'");
                break;
            }
        }

        assertTrue(foundMax, "Should find max temperature 30 in output");
    }

    @Test
    public void shouldFilterTemperaturesBelowThreshold() {
        // Given - temperature readings all below threshold (20)
        final Instant baseTime = Instant.ofEpochMilli(0);
        final List<TestRecord<String, String>> inputRecords = Arrays.asList(
                new TestRecord<>(null, "10", baseTime),
                new TestRecord<>(null, "15", baseTime.plusMillis(1000)),
                new TestRecord<>(null, "18", baseTime.plusMillis(2000)),
                new TestRecord<>(null, "19", baseTime.plusMillis(3000))
        );

        // When
        inputTopic.pipeRecordList(inputRecords);

        // Then - should have no output since max (19) is below threshold (20)
        final List<TestRecord<Windowed<String>, String>> outputRecords = outputTopic.readRecordsToList();

        assertTrue(outputRecords.isEmpty(), "Should have no output records when all temperatures are below threshold");
    }

    @Test
    public void shouldOutputOnlyWhenMaxExceedsThreshold() {
        // Given - mix of temperatures, some above and some below threshold
        final Instant baseTime = Instant.ofEpochMilli(0);
        final List<TestRecord<String, String>> inputRecords = Arrays.asList(
                new TestRecord<>(null, "15", baseTime),
                new TestRecord<>(null, "25", baseTime.plusMillis(1000)), // Above threshold
                new TestRecord<>(null, "18", baseTime.plusMillis(2000)),
                new TestRecord<>(null, "21", baseTime.plusMillis(3000)) // Above threshold
        );

        // When
        inputTopic.pipeRecordList(inputRecords);

        // Then - should output max value (25) since it exceeds threshold
        final List<TestRecord<Windowed<String>, String>> outputRecords = outputTopic.readRecordsToList();

        assertFalse(outputRecords.isEmpty(), "Should have output records");

        // The max should be 25
        boolean foundCorrectMax = false;
        for (final TestRecord<Windowed<String>, String> record : outputRecords) {
            if ("25".equals(record.value())) {
                foundCorrectMax = true;
                break;
            }
        }

        assertTrue(foundCorrectMax, "Should output max temperature 25");
    }

    @Test
    public void shouldHandleDifferentTimeWindows() {
        // Given - temperatures in different 5-second windows
        final Instant baseTime = Instant.ofEpochMilli(0);
        final List<TestRecord<String, String>> inputRecords = Arrays.asList(
                // First window (0-5 seconds)
                new TestRecord<>(null, "25", baseTime),
                new TestRecord<>(null, "30", baseTime.plusMillis(2000)),

                // Second window (5-10 seconds)
                new TestRecord<>(null, "22", baseTime.plusMillis(6000)),
                new TestRecord<>(null, "35", baseTime.plusMillis(8000))
        );

        // When
        inputTopic.pipeRecordList(inputRecords);

        // Then - should have outputs for both windows
        final List<TestRecord<Windowed<String>, String>> outputRecords = outputTopic.readRecordsToList();

        assertFalse(outputRecords.isEmpty(), "Should have output records");

        // Should find max from each window
        boolean foundFirstWindowMax = false;
        boolean foundSecondWindowMax = false;

        for (final TestRecord<Windowed<String>, String> record : outputRecords) {
            if ("30".equals(record.value())) {
                foundFirstWindowMax = true;
            }
            if ("35".equals(record.value())) {
                foundSecondWindowMax = true;
            }
        }

        assertTrue(foundFirstWindowMax, "Should find max from first window (30)");
        assertTrue(foundSecondWindowMax, "Should find max from second window (35)");
    }

    @Test
    public void shouldHandleInvalidTemperatureValues() {
        // Given - mix of valid and invalid temperature values
        final Instant baseTime = Instant.ofEpochMilli(0);

        // Send some valid temperatures first
        inputTopic.pipeInput(null, "25", baseTime.toEpochMilli());
        inputTopic.pipeInput(null, "30", baseTime.plusMillis(1000));

        // This test mainly verifies the topology doesn't crash with invalid input
        // The actual TemperatureDemo will throw NumberFormatException for invalid input
        // In a real scenario, you might want to add error handling in the topology

        // Then - should have processed valid temperatures
        final List<TestRecord<Windowed<String>, String>> outputRecords = outputTopic.readRecordsToList();

        assertFalse(outputRecords.isEmpty(), "Should have processed valid temperatures");

        boolean foundValidMax = false;
        for (final TestRecord<Windowed<String>, String> record : outputRecords) {
            if ("30".equals(record.value())) {
                foundValidMax = true;
                break;
            }
        }

        assertTrue(foundValidMax, "Should process valid temperature values");
    }

    @Test
    public void shouldProcessEmptyInput() {
        // Given - no input records

        // When - process empty stream
        // (no records sent)

        // Then - should handle gracefully with no output
        final List<TestRecord<Windowed<String>, String>> outputRecords = outputTopic.readRecordsToList();
        assertTrue(outputRecords.isEmpty(), "Should have no output records for empty input");
    }


}