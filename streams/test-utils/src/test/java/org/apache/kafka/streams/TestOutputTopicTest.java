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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class TestOutputTopicTest {

    private final static String INPUT_TOPIC = "input";
    private final static String OUTPUT_TOPIC = "output1";
    private final static String INPUT_TOPIC_MAP = OUTPUT_TOPIC;
    private final static String OUTPUT_TOPIC_MAP = "output2";

    private TopologyTestDriver testDriver;
    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    private final Properties config = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "TestOutputTopicTest"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    ));

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline
        builder.stream(INPUT_TOPIC).to(OUTPUT_TOPIC);
        final KStream<Long, String> source = builder.stream(INPUT_TOPIC_MAP, Consumed.with(longSerde, stringSerde));
        final KStream<String, Long> mapped = source.map((key, value) -> new KeyValue<>(value, key));
        mapped.to(OUTPUT_TOPIC_MAP, Produced.with(stringSerde, longSerde));
        testDriver = new TopologyTestDriver(builder.build(), config);
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    public void testReadValue() {
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeInput("Hello");
        assertThat(outputTopic.readValue(), equalTo("Hello"));
        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void testValueList() {
        //Note using here string key serde even other topic is expecting long
        //Does not affect when key not used
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        //Feed list of words to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeValueList(inputList);
        final List<String> output = outputTopic.readValuesToList();
        assertThat(output, hasItems("This", "is", "an", "example"));
        assertThat(output, is(equalTo(inputList)));
    }

    @Test
    public void testReadKeyValue() {
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<Long, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        inputTopic.pipeInput(1L, "Hello");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>(1L, "Hello")));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void testKeyValueList() {
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
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
        assertThat(output, is(equalTo(expected)));
    }

    @Test
    public void testKeyValuesToMap() {
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
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
        assertThat(output, is(equalTo(expected)));
    }

    @Test
    public void testMultipleTopics() {
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<Long, String> outputTopic1 = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        final TestOutputTopic<String, Long> outputTopic2 = testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
        inputTopic.pipeInput(1L, "Hello");
        assertThat(outputTopic1.isEmpty(), is(false));
        assertThat(outputTopic2.isEmpty(), is(false));
        assertThat(outputTopic1.readKeyValue(), equalTo(new KeyValue<>(1L, "Hello")));
        assertThat(outputTopic2.readKeyValue(), equalTo(new KeyValue<>("Hello", 1L)));
        assertThat(outputTopic1.isEmpty(), is(true));
        assertThat(outputTopic2.isEmpty(), is(true));
    }

    @Test
    public void testNonExistingTopic() {
        final TestOutputTopic<Long, String> outputTopic = testDriver.createOutputTopic("no-exist", longSerde.deserializer(), stringSerde.deserializer());
        assertThrows("Unknown topic", IllegalArgumentException.class, () -> outputTopic.readRecord());
    }

    @Test
    public void testNonUsedTopic() {
        final TestOutputTopic<Long, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());
        assertThrows("Uninitialized topic", NoSuchElementException.class, () -> outputTopic.readRecord());
    }

    @Test
    public void testEmptyTopic() {
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeInput("Hello");
        assertThat(outputTopic.readValue(), equalTo("Hello"));
        //No more output in topic
        assertThrows("Empty topic", NoSuchElementException.class, () -> outputTopic.readRecord());
    }


    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullTopicName() {
        final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(null, stringSerde.deserializer(), stringSerde.deserializer());
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateWithNullDriver() {
        final TestOutputTopic<String, String> outputTopic = new TestOutputTopic<>(null, OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
    }

    @Test(expected = SerializationException.class)
    public void testWrongSerde() {
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<Long, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, longSerde.deserializer(), stringSerde.deserializer());
        inputTopic.pipeInput(1L, "Hello");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>(1L, "Hello")));
    }

    @Test
    public void testToString() {
        final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), stringSerde.deserializer());
        assertThat(outputTopic.toString(), equalTo("TestOutputTopic{topic='output1',size=0}"));
    }

    @Test
    public void testRecordsToList() {
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC_MAP, longSerde.serializer(), stringSerde.serializer());
        final TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde.deserializer(), longSerde.deserializer());
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
        assertThat(output, is(equalTo(expected)));
    }
}
