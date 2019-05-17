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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.OutputVerifier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class TestInputTopicTest {

    private final static String INPUT_TOPIC = "input";
    private final static String OUTPUT_TOPIC = "output1";
    private final static String INPUT_TOPIC_MAP = OUTPUT_TOPIC;
    private final static String OUTPUT_TOPIC_MAP = "output2";

    private TopologyTestDriver testDriver;
    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    private final Properties config = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "TestInputTopicTest"),
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
    public void testValue() {
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic( INPUT_TOPIC, stringSerde, stringSerde);
        final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde, stringSerde);
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeInput("Hello");
        assertThat(outputTopic.readValue(), equalTo("Hello"));
        //No more output in topic
        assertThat(outputTopic.readRecord(), nullValue());
    }

    @Test
    public void testValueList() {
        //Note using here string key serde even other topic is expecting long
        //Does not affect when key not used
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic( INPUT_TOPIC, stringSerde, stringSerde);
        final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde, stringSerde);
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        //Feed list of words to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeValueList(inputList);
        final List<String> output = outputTopic.readValuesToList();
        assertThat(output, hasItems("This", "is", "an", "example"));
        assertThat(output, is(equalTo(inputList)));
    }

    @Test
    public void testKeyValue() {
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic( INPUT_TOPIC, longSerde, stringSerde);
        final TestOutputTopic<Long, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde, stringSerde);
        inputTopic.pipeInput(1L, "Hello");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>(1L, "Hello")));
        assertThat(outputTopic.readRecord(), nullValue());
    }

    @Test
    public void testKeyValueList() {
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic( INPUT_TOPIC_MAP, longSerde, stringSerde);
        final TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde, longSerde);
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
    public void testTimestampMs() {
        long baseTime = 3;
        final long advance = 2;
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic( INPUT_TOPIC, longSerde, stringSerde);
        final TestOutputTopic<Long, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde, stringSerde);
        inputTopic.pipeInput("Hello", baseTime);
        OutputVerifier.compareValueTimestamp(outputTopic.readRecord(), "Hello", baseTime);
        inputTopic.pipeInput(2L, "Kafka", ++baseTime);
        OutputVerifier.compareKeyValueTimestamp(outputTopic.readRecord(), 2L, "Kafka", baseTime);

        final List<String> inputList = Arrays.asList("Advancing", "time");
        //Feed list of words to inputTopic and no kafka key, timestamp advancing from basetime
        baseTime = 10;
        inputTopic.pipeValueList(inputList, baseTime, advance);
        OutputVerifier.compareValueTimestamp(outputTopic.readRecord(), "Advancing", baseTime);
        OutputVerifier.compareValueTimestamp(outputTopic.readRecord(), "time", baseTime + advance);
    }

    @Test
    public void testWithHeaders() {
        final long baseTime = 3;
        final Headers headers = new RecordHeaders(
                new Header[]{
                    new RecordHeader("foo", "value".getBytes()),
                    new RecordHeader("bar", (byte[]) null),
                    new RecordHeader("\"A\\u00ea\\u00f1\\u00fcC\"", "value".getBytes())
                });
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic( INPUT_TOPIC, longSerde, stringSerde);
        final TestOutputTopic<Long, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde, stringSerde);
        inputTopic.pipeInput(1L, "Hello", headers);
        OutputVerifier.compareKeyValueHeaders(outputTopic.readRecord(), 1L, "Hello", headers);
        inputTopic.pipeInput(2L, "Kafka", headers, baseTime);
        OutputVerifier.compareKeyValueHeadersTimestamp(outputTopic.readRecord(), 2L, "Kafka", headers, baseTime);
    }

    @Test
    public void testStartTimestampMs() {
        final long baseTime = 3;
        final long advance = 2;
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, longSerde, stringSerde);
        inputTopic.configureTiming(baseTime);
        final TestOutputTopic<Long, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde, stringSerde);
        inputTopic.pipeInput(1L, "Hello");
        OutputVerifier.compareKeyValueTimestamp(outputTopic.readRecord(), 1L, "Hello", baseTime);
        inputTopic.pipeInput(2L, "World");
        OutputVerifier.compareKeyValueTimestamp(outputTopic.readRecord(), 2L, "World", baseTime);
        inputTopic.advanceTimeMs(advance);
        inputTopic.pipeInput(3L, "Kafka");
        OutputVerifier.compareKeyValueTimestamp(outputTopic.readRecord(), 3L, "Kafka", baseTime+advance);
    }


    @Test
    public void testTimestampMsAutoAdvance() {
        final long baseTime = 3;
        final long advance = 2;
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic( INPUT_TOPIC, longSerde, stringSerde);
        inputTopic.configureTiming(baseTime, advance);
        final TestOutputTopic<Long, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde, stringSerde);
        inputTopic.pipeInput("Hello");
        OutputVerifier.compareValueTimestamp(outputTopic.readRecord(), "Hello", baseTime);
        inputTopic.pipeInput(2L, "Kafka");
        OutputVerifier.compareKeyValueTimestamp(outputTopic.readRecord(), 2L, "Kafka", baseTime + advance);
    }


    @Test
    public void testMultipleTopics() {
        final TestInputTopic<Long, String> inputTopic1 = testDriver.createInputTopic( INPUT_TOPIC, longSerde, stringSerde);
        final TestInputTopic<Long, String> inputTopic2 = testDriver.createInputTopic( INPUT_TOPIC_MAP, longSerde, stringSerde);
        final TestOutputTopic<Long, String> outputTopic1 = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde, stringSerde);
        final TestOutputTopic<String, Long> outputTopic2 = testDriver.createOutputTopic(OUTPUT_TOPIC_MAP, stringSerde, longSerde);
        inputTopic1.pipeInput(1L, "Hello");
        assertThat(outputTopic1.readKeyValue(), equalTo(new KeyValue<>(1L, "Hello")));
        assertThat(outputTopic2.readKeyValue(), equalTo(new KeyValue<>("Hello", 1L)));
        assertThat(outputTopic1.readRecord(), nullValue());
        assertThat(outputTopic2.readRecord(), nullValue());
        inputTopic2.pipeInput(1L, "Hello");
        //This is not visible in outputTopic1 even it is the same topic
        assertThat(outputTopic2.readKeyValue(), equalTo(new KeyValue<>("Hello", 1L)));
        assertThat(outputTopic1.readRecord(), nullValue());
        assertThat(outputTopic2.readRecord(), nullValue());
    }

    @Test
    public void testNonExistingTopic() {
        final TestInputTopic<Long, String> inputTopic = testDriver.createInputTopic( "no-exist", longSerde, stringSerde);
        assertThrows("Unknown topic", IllegalArgumentException.class, () -> inputTopic.pipeInput(1L, "Hello"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullTopicName() {
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic( null, stringSerde, stringSerde);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateWithNullDriver() {
        final TestInputTopic<String, String> inputTopic = new TestInputTopic<>(null, INPUT_TOPIC, stringSerde, stringSerde);
    }


    @Test(expected = StreamsException.class)
    public void testWrongSerde() {
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic( INPUT_TOPIC_MAP, stringSerde, stringSerde);
        inputTopic.pipeInput("1L", "Hello");
    }

    @Test
    public void testToString() {
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic( "topicName", stringSerde, stringSerde);
        assertThat(inputTopic.toString(), equalTo("TestInputTopic{topic='topicName'}"));
    }
}
