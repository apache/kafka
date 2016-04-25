/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class KGroupedTableImplTest {

    private File stateDir;

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGroupedCountOccurences() throws IOException {
        final KStreamBuilder builder = new KStreamBuilder();
        final String input = "count-test-input";
        final MockProcessorSupplier processorSupplier = new MockProcessorSupplier<>();

        builder.table(Serdes.String(), Serdes.String(), input)
                .groupBy(new KeyValueMapper<String, String, KeyValue<String, String>>() {
                    @Override
                    public KeyValue<String, String> apply(final String key, final String value) {
                        return new KeyValue<>(value, value);
                    }
                }, Serdes.String(), Serdes.String())
                .count("count")
                .toStream()
                .process(processorSupplier);


        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);


        driver.process(input, "A", "green");
        driver.process(input, "B", "green");
        driver.process(input, "A", "blue");
        driver.process(input, "C", "yellow");
        driver.process(input, "D", "green");

        final List<String> expected = Arrays.asList("green:1", "green:2", "blue:1", "green:1", "yellow:1", "green:2");
        final List<String> actual = processorSupplier.processed;
        assertEquals(expected, actual);
    }
}