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
package org.apache.kafka.streams;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTopicsTest {
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output1";
    private static final String INPUT_TOPIC_MAP = OUTPUT_TOPIC;
    private static final String OUTPUT_TOPIC_MAP = "output2";

    private TopologyTestDriver testDriver;
    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    private final Instant testBaseTime = Instant.parse("2019-06-01T10:00:00Z");

    @BeforeEach
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline
        builder.stream(INPUT_TOPIC).to(OUTPUT_TOPIC);
        final KStream<Long, String> source = builder.stream(INPUT_TOPIC_MAP, Consumed.with(longSerde, stringSerde));
        final KStream<String, Long> mapped = source.map((key, value) -> new KeyValue<>(value, key));
        mapped.to(OUTPUT_TOPIC_MAP, Produced.with(stringSerde, longSerde));

        final Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        testDriver = new TopologyTestDriverBuilder(builder.build()).withConfig(properties).build();
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testValue() {
        final TestInputTopic<String, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeInput("Hello");
        assertEquals("Hello", outputTopic.readValue());
        //No more output in topic
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void testValueList() {
        final TestInputTopic<String, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        //Feed list of words to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeValueList(inputList);
        final List<String> output = outputTopic.readValuesToList();
        assertEquals(inputList, output);
    }

    @Test
    public void testKeyValue() {
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<Long, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        inputTopic.pipeInput(1L, "Hello");
        assertEquals(new KeyValue<>(1L, "Hello"), outputTopic.readKeyValue());
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void testKeyValueList() {
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, Long> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        final List<KeyValue<Long, String>> input = new LinkedList<>();
        final List<KeyValue<String, Long>> expected = new LinkedList<>();
        long i = 0;
        for (final String s : inputList) {
            input.add(new KeyValue<>(i, s));
            expected.add(new KeyValue<>(s, i));
            i++;
        }
        inputTopic.pipeKeyValueList(input);
        final List<KeyValue<String, Long>> output = outputTopic.readKeyValuesToList();
        assertEquals(expected, output);
    }

    @Test
    public void testKeyValuesToMap() {
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, Long> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        final List<KeyValue<Long, String>> input = new LinkedList<>();
        final Map<String, Long> expected = new HashMap<>();
        long i = 0;
        for (final String s : inputList) {
            input.add(new KeyValue<>(i, s));
            expected.put(s, i);
            i++;
        }
        inputTopic.pipeKeyValueList(input);
        final Map<String, Long> output = outputTopic.readKeyValuesToMap();
        assertEquals(expected, output);
    }

    @Test
    public void testKeyValuesToMapWithNull() {
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<Long, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        inputTopic.pipeInput("value");
        assertThrows(IllegalStateException.class, outputTopic::readKeyValuesToMap);
    }

    @Test
    public void testKeyValueListDuration() {
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, Long> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        final List<KeyValue<Long, String>> input = new LinkedList<>();
        final List<TestRecord<String, Long>> expected = new LinkedList<>();
        long i = 0;
        final Duration advance = Duration.ofSeconds(15);
        Instant recordInstant = testBaseTime;
        for (final String s : inputList) {
            input.add(new KeyValue<>(i, s));
            expected.add(new TestRecord<>(s, i, recordInstant));
            i++;
            recordInstant = recordInstant.plus(advance);
        }
        inputTopic.pipeKeyValueList(input, testBaseTime, advance);
        final List<TestRecord<String, Long>> output = outputTopic.readRecordsToList();
        assertEquals(expected, output);
    }

    @Test
    public void testRecordList() {
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, Long> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        final List<TestRecord<Long, String>> input = new LinkedList<>();
        final List<TestRecord<String, Long>> expected = new LinkedList<>();
        final Duration advance = Duration.ofSeconds(15);
        Instant recordInstant = testBaseTime;
        Long i = 0L;
        for (final String s : inputList) {
            input.add(new TestRecord<>(i, s, recordInstant));
            expected.add(new TestRecord<>(s, i, recordInstant));
            i++;
            recordInstant = recordInstant.plus(advance);
        }
        inputTopic.pipeRecordList(input);
        final List<TestRecord<String, Long>> output = outputTopic.readRecordsToList();
        assertEquals(expected, output);
    }

    @Test
    public void testTimestamp() {
        long baseTime = 3;
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<Long, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        inputTopic.pipeInput(null, "Hello", baseTime);
        assertEquals(new TestRecord<>(null, "Hello", null, baseTime), outputTopic.readRecord());

        inputTopic.pipeInput(2L, "Kafka", ++baseTime);
        assertEquals(new TestRecord<>(2L, "Kafka", null, baseTime), outputTopic.readRecord());

        inputTopic.pipeInput(2L, "Kafka", testBaseTime);
        assertEquals(new TestRecord<>(2L, "Kafka", testBaseTime), outputTopic.readRecord());

        final List<String> inputList = Arrays.asList("Advancing", "time");
        //Feed list of words to inputTopic and no kafka key, timestamp advancing from testInstant
        final Duration advance = Duration.ofSeconds(15);
        final Instant recordInstant = testBaseTime.plus(Duration.ofDays(1));
        inputTopic.pipeValueList(inputList, recordInstant, advance);
        assertEquals(new TestRecord<>(null, "Advancing", recordInstant), outputTopic.readRecord());
        assertEquals(new TestRecord<>(null, "time", null, recordInstant.plus(advance)), outputTopic.readRecord());
    }

    @Test
    public void testWithHeaders() {
        long baseTime = 3;
        final Headers headers = new RecordHeaders(
                new Header[]{
                    new RecordHeader("foo", "value".getBytes()),
                    new RecordHeader("bar", null),
                    new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
                });
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<Long, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        inputTopic.pipeInput(new TestRecord<>(1L, "Hello", headers));
        final TestRecord<Long, String> headerRecord = outputTopic.readRecord();
        assertEquals(1L, headerRecord.getKey());
        assertEquals("Hello", headerRecord.getValue());
        assertEquals(headers, headerRecord.getHeaders());
        inputTopic.pipeInput(new TestRecord<>(2L, "Kafka", headers, ++baseTime));
        assertEquals(new TestRecord<>(2L, "Kafka", headers, baseTime), outputTopic.readRecord());
    }

    @Test
    public void testStartTimestamp() {
        final Duration advance = Duration.ofSeconds(2);
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer(), testBaseTime, Duration.ZERO);
        final TestOutputTopic<Long, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        inputTopic.pipeInput(1L, "Hello");
        assertEquals(new TestRecord<>(1L, "Hello", testBaseTime), outputTopic.readRecord());
        inputTopic.pipeInput(2L, "World");
        assertEquals(new TestRecord<>(2L, "World", null, testBaseTime.toEpochMilli()), outputTopic.readRecord());
        inputTopic.advanceTime(advance);
        inputTopic.pipeInput(3L, "Kafka");
        assertEquals(new TestRecord<>(3L, "Kafka", testBaseTime.plus(advance)), outputTopic.readRecord());
    }

    @Test
    public void testTimestampAutoAdvance() {
        final Duration advance = Duration.ofSeconds(2);
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer(), testBaseTime, advance);
        final TestOutputTopic<Long, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        inputTopic.pipeInput("Hello");
        assertEquals(new TestRecord<>(null, "Hello", testBaseTime), outputTopic.readRecord());
        inputTopic.pipeInput(2L, "Kafka");
        assertEquals(new TestRecord<>(2L, "Kafka", testBaseTime.plus(advance)), outputTopic.readRecord());
    }


    @Test
    public void testMultipleTopics() {
        final TestInputTopic<Long, String> inputTopic1 =
            testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer());
        final TestInputTopic<Long, String> inputTopic2 =
            testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<Long, String> outputTopic1 =
            testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        final TestOutputTopic<String, Long> outputTopic2 =
            testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
        inputTopic1.pipeInput(1L, "Hello");
        assertEquals(new KeyValue<>(1L, "Hello"), outputTopic1.readKeyValue());
        assertEquals(new KeyValue<>("Hello", 1L), outputTopic2.readKeyValue());
        assertTrue(outputTopic1.isEmpty());
        assertTrue(outputTopic2.isEmpty());
        inputTopic2.pipeInput(1L, "Hello");
        //This is not visible in outputTopic1 even it is the same topic
        assertEquals(new KeyValue<>("Hello", 1L), outputTopic2.readKeyValue());
        assertTrue(outputTopic1.isEmpty());
        assertTrue(outputTopic2.isEmpty());
    }

    @Test
    public void testNonExistingOutputTopic() {
        final TestOutputTopic<Long, String> outputTopic =
            testDriver.createOutputTopic("no-exist", longSerde.deserializer(), stringSerde.deserializer());
        assertThrows(NoSuchElementException.class, outputTopic::readRecord, "Uninitialized topic");
    }

    @Test
    public void testNonUsedOutputTopic() {
        final TestOutputTopic<Long, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        assertThrows(NoSuchElementException.class, outputTopic::readRecord, "Uninitialized topic");
    }

    @Test
    public void testEmptyTopic() {
        final TestInputTopic<String, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeInput("Hello");
        assertEquals("Hello", outputTopic.readValue());
        //No more output in topic
        assertThrows(NoSuchElementException.class, outputTopic::readRecord, "Empty topic");
    }

    @Test
    public void testNonExistingInputTopic() {
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic("no-exist", longSerde.serializer(), stringSerde.serializer());
        assertThrows(IllegalArgumentException.class, () -> inputTopic.pipeInput(1L, "Hello"), "Unknown topic");
    }

    @Test
    public void shouldNotAllowToCreateTopicWithNullTopicName() {
        assertThrows(NullPointerException.class, () -> testDriver.createInputTopic(null, stringSerde.serializer(), stringSerde.serializer()));
    }

    @Test
    public void shouldNotAllowToCreateWithNullDriver() {
        assertThrows(NullPointerException.class,
            () -> new TestInputTopic<>(null, INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer(), Instant.now(), Duration.ZERO));
    }


    @Test
    public void testWrongSerde() {
        final TestInputTopic<String, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC_MAP, stringSerde.serializer(), stringSerde.serializer());
        assertThrows(StreamsException.class, () -> inputTopic.pipeInput("1L", "Hello"));
    }

    @Test
    public void testDuration() {
        assertThrows(IllegalArgumentException.class,
            () -> testDriver.createInputTopic(INPUT_TOPIC_MAP, stringSerde.serializer(), stringSerde.serializer(), testBaseTime, Duration.ofDays(-1)));
    }

    @Test
    public void testNegativeAdvance() {
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC_MAP, stringSerde.serializer(), stringSerde.serializer());
        assertThrows(IllegalArgumentException.class, () -> inputTopic.advanceTime(Duration.ofDays(-1)));
    }

    @Test
    public void testInputToString() {
        final TestInputTopic<String, String> inputTopic =
            testDriver.createInputTopic("topicName", stringSerde.serializer(), stringSerde.serializer());
        final String inputTopicString = inputTopic.toString();
        assertTrue(inputTopicString.contains("TestInputTopic"));
        assertTrue(inputTopicString.contains("topic='topicName'"));
        assertTrue(inputTopicString.contains("StringSerializer"));
    }

    @Test
    public void shouldNotAllowToCreateOutputTopicWithNullTopicName() {
        assertThrows(NullPointerException.class, () -> testDriver.createOutputTopic(null, stringSerde.deserializer(), stringSerde.deserializer()));
    }

    @Test
    public void shouldNotAllowToCreateOutputWithNullDriver() {
        assertThrows(NullPointerException.class, () -> new TestOutputTopic<>(null, OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer()));
    }

    @Test
    public void testOutputWrongSerde() {
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<Long, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, longSerde.deserializer(), stringSerde.deserializer());
        inputTopic.pipeInput(1L, "Hello");
        assertThrows(SerializationException.class, outputTopic::readKeyValue);
    }

    @Test
    public void testOutputToString() {
        final TestOutputTopic<String, String> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
        final String outputTopicString = outputTopic.toString();
        assertTrue(outputTopicString.contains("TestOutputTopic"));
        assertTrue(outputTopicString.contains("topic='output1'"));
        assertTrue(outputTopicString.contains("size=0"));
        assertTrue(outputTopicString.contains("StringDeserializer"));
    }

    @Test
    public void testRecordsToList() {
        final TestInputTopic<Long, String> inputTopic =
            testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, Long> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        final List<KeyValue<Long, String>> input = new LinkedList<>();
        final List<TestRecord<String, Long>> expected = new LinkedList<>();
        long i = 0;
        final Duration advance = Duration.ofSeconds(15);
        Instant recordInstant = Instant.parse("2019-06-01T10:00:00Z");
        for (final String s : inputList) {
            input.add(new KeyValue<>(i, s));
            expected.add(new TestRecord<>(s, i, recordInstant));
            i++;
            recordInstant = recordInstant.plus(advance);
        }
        inputTopic.pipeKeyValueList(input, Instant.parse("2019-06-01T10:00:00Z"), advance);
        final List<TestRecord<String, Long>> output = outputTopic.readRecordsToList();
        assertEquals(expected, output);
    }

}
