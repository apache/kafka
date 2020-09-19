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
package org.apache.kafka.streams.examples.wordcount;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit test of {@link WordCountDemo} stream using TopologyTestDriver.
 */
public class WordCountDemoTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @Before
    public void setup() throws IOException {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline
        WordCountDemo.createWordCountStream(builder);
        testDriver = new TopologyTestDriver(builder.build(), WordCountDemo.getStreamsConfig(null));
        inputTopic = testDriver.createInputTopic(WordCountDemo.INPUT_TOPIC, new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic(WordCountDemo.OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());
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


    /**
     * Simple test validating count of one word
     */
    @Test
    public void testOneWord() {
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeInput("Hello");
        //Read and validate output to match word as key and count as value
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("hello", 1L)));
        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));
    }

    /**
     * Test Word count of sentence list.
     */
    @Test
    public void testCountListOfWords() {
        final List<String> inputValues = Arrays.asList(
                "Apache Kafka Streams Example",
                "Using Kafka Streams Test Utils",
                "Reading and Writing Kafka Topic"
        );
        final Map<String, Long> expectedWordCounts = new HashMap<>();
        expectedWordCounts.put("apache", 1L);
        expectedWordCounts.put("kafka", 3L);
        expectedWordCounts.put("streams", 2L);
        expectedWordCounts.put("example", 1L);
        expectedWordCounts.put("using", 1L);
        expectedWordCounts.put("test", 1L);
        expectedWordCounts.put("utils", 1L);
        expectedWordCounts.put("reading", 1L);
        expectedWordCounts.put("and", 1L);
        expectedWordCounts.put("writing", 1L);
        expectedWordCounts.put("topic", 1L);

        inputTopic.pipeValueList(inputValues);
        final Map<String, Long> actualWordCounts = outputTopic.readKeyValuesToMap();
        assertThat(actualWordCounts, equalTo(expectedWordCounts));
    }

    @Test
    public void testGetStreamsConfig() throws IOException {
        final File tmp = TestUtils.tempFile("bootstrap.servers=localhost:1234");
        try {
            Properties config = WordCountDemo.getStreamsConfig(new String[] {tmp.getPath()});
            assertThat("localhost:1234", equalTo(config.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)));

            config = WordCountDemo.getStreamsConfig(new String[] {tmp.getPath(), "extra", "args"});
            assertThat("localhost:1234", equalTo(config.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)));
        } finally {
            Files.deleteIfExists(tmp.toPath());
        }
    }

}
