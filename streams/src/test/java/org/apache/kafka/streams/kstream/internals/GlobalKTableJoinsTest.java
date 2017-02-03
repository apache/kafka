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
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class GlobalKTableJoinsTest {

    private final KStreamBuilder builder = new KStreamBuilder();
    private final Map<String, String> results = new HashMap<>();
    private final String streamTopic = "stream";
    private final String globalTopic = "global";
    private File stateDir;
    private GlobalKTable<String, String> global;
    private KStream<String, String> stream;
    private KeyValueMapper<String, String, String> keyValueMapper;
    private ForeachAction<String, String> action;
    private KStreamTestDriver driver = null;

    @Before
    public void setUp() throws Exception {
        stateDir = TestUtils.tempDirectory();
        global = builder.globalTable(Serdes.String(), Serdes.String(), globalTopic, "global-store");
        stream = builder.stream(Serdes.String(), Serdes.String(), streamTopic);
        keyValueMapper = new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(final String key, final String value) {
                return value;
            }
        };
        action = new ForeachAction<String, String>() {
            @Override
            public void apply(final String key, final String value) {
                results.put(key, value);
            }
        };
    }

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void shouldLeftJoinWithStream() throws Exception {
        stream.leftJoin(global, keyValueMapper, MockValueJoiner.TOSTRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");
        expected.put("3", "c+null");

        verifyJoin(expected, streamTopic);

    }

    @Test
    public void shouldInnerJoinWithStream() throws Exception {
        stream.join(global, keyValueMapper,  MockValueJoiner.TOSTRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");

        verifyJoin(expected, streamTopic);
    }

    private void verifyJoin(final Map<String, String> expected, final String joinInput) {
        driver = new KStreamTestDriver(builder, stateDir);
        driver.setTime(0L);
        // write some data to the global table
        driver.process(globalTopic, "a", "A");
        driver.process(globalTopic, "b", "B");
        //write some data to the stream
        driver.process(joinInput, "1", "a");
        driver.process(joinInput, "2", "b");
        driver.process(joinInput, "3", "c");
        driver.flushState();

        assertEquals(expected, results);
    }
}
