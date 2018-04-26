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

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;

public class KStreamMapValuesTest {

    private String topicName = "topic";

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();
    private final ConsumerRecordFactory<Integer, String> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new StringSerializer());
    private TopologyTestDriver driver;
    private final Properties props = new Properties();

    @Before
    public void setup() {
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-map-values-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }

    @After
    public void cleanup() {
        props.clear();
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testFlatMapValues() {
        StreamsBuilder builder = new StreamsBuilder();

        ValueMapper<CharSequence, Integer> mapper =
            new ValueMapper<CharSequence, Integer>() {
                @Override
                public Integer apply(CharSequence value) {
                    return value.length();
                }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        KStream<Integer, String> stream;
        MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        stream = builder.stream(topicName, Consumed.with(intSerde, stringSerde));
        stream.mapValues(mapper).process(processor);

        driver = new TopologyTestDriver(builder.build(), props);
        for (int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topicName, expectedKey, Integer.toString(expectedKey)));
        }
        String[] expected = {"1:1", "10:2", "100:3", "1000:4"};

        assertArrayEquals(expected, processor.processed.toArray());
    }

    @Test
    public void testMapValuesWithKeys() {
        StreamsBuilder builder = new StreamsBuilder();

        ValueMapperWithKey<Integer, CharSequence, Integer> mapper =
                new ValueMapperWithKey<Integer, CharSequence, Integer>() {
            @Override
            public Integer apply(final Integer readOnlyKey, final CharSequence value) {
                return value.length() + readOnlyKey;
            }
        };

        final int[] expectedKeys = {1, 10, 100, 1000};

        KStream<Integer, String> stream;
        MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        stream = builder.stream(topicName, Consumed.with(intSerde, stringSerde));
        stream.mapValues(mapper).process(processor);

        driver = new TopologyTestDriver(builder.build(), props);
        for (int expectedKey : expectedKeys) {
            driver.pipeInput(recordFactory.create(topicName, expectedKey, Integer.toString(expectedKey)));
        }
        String[] expected = {"1:2", "10:12", "100:103", "1000:1004"};

        assertArrayEquals(expected, processor.processed.toArray());
    }

}
