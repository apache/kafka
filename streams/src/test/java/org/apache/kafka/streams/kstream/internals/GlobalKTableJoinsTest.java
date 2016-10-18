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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class GlobalKTableJoinsTest {

    private final KStreamBuilder builder = new KStreamBuilder();
    private GlobalKTable<String, String> global;
    private File stateDir;
    private final Map<String, String> results = new HashMap<>();
    private KStream<String, String> stream;
    private KeyValueMapper<String, String, String> keyValueMapper;
    private ForeachAction<String, String> action;
    private KTable<String, String> table;
    private final String streamTopic = "stream";
    private final String globalTopic = "global";
    private final String tableTopic = "table";
    private final String global2Topic = "global2";
    private GlobalKTable<String, String> global2;

    @Before
    public void setUp() throws Exception {
        stateDir = TestUtils.tempDirectory();
        global = builder.globalTable(Serdes.String(), Serdes.String(), globalTopic, "global-store");
        stream = builder.stream(Serdes.String(), Serdes.String(), streamTopic);
        global2 = builder.globalTable(Serdes.String(), Serdes.String(), global2Topic, "global-store-2");
        table = builder.table(Serdes.String(), Serdes.String(), tableTopic, tableTopic);

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

    @Test
    public void shouldLeftJoinWithStream() throws Exception {
        stream.leftJoin(global, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");
        expected.put("3", "c+null");

        verifyJoin(expected, streamTopic);

    }

    @Test
    public void shouldInnerJoinWithStream() throws Exception {
        stream.join(global, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");

        verifyJoin(expected, streamTopic);
    }

    @Test
    public void shouldLeftJoinWithTable() throws Exception {
        table.leftJoin(global, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");
        expected.put("3", "c+null");

        verifyJoin(expected, tableTopic);
    }

    @Test
    public void shouldJoinWithTable() throws Exception {
        table.join(global, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");

        verifyJoin(expected, tableTopic);
    }

    @Test
    public void shouldJoinWithGlobalTableAndThenStream() throws Exception {
        final GlobalKTable<String, String> joined = global.join(global2, keyValueMapper, MockValueJoiner.STRING_JOINER, "view");

        stream.join(joined, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = Collections.singletonMap("1", "a+A+B");
        verifyGlobalGlobalJoin(expected, streamTopic);
    }

    @Test
    public void shouldJoinWithGlobalTableAndThenTable() throws Exception {
        final GlobalKTable<String, String> joined = global.join(global2, keyValueMapper, MockValueJoiner.STRING_JOINER, "view");

        table.join(joined, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = Collections.singletonMap("1", "a+A+B");
        verifyGlobalGlobalJoin(expected, tableTopic);
    }

    @Test
    public void shouldLeftJoinWithGlobalTableAndThenStream() throws Exception {
        final GlobalKTable<String, String> joined = global.leftJoin(global2, keyValueMapper, MockValueJoiner.STRING_JOINER, "view");
        stream.join(joined, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A+B");
        expected.put("2", "b+B+null");

        verifyGlobalGlobalLeftJoin(expected, streamTopic);
    }

    @Test
    public void shouldLeftJoinWithGlobalTableAndThenTable() throws Exception {
        final GlobalKTable<String, String> joined = global.leftJoin(global2, keyValueMapper, MockValueJoiner.STRING_JOINER, "view");
        table.join(joined, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A+B");
        expected.put("2", "b+B+null");

        verifyGlobalGlobalLeftJoin(expected, tableTopic);
    }

    @Test
    public void shouldCreateStateStoreForGlobalTableJoin() throws Exception {
        global.leftJoin(global2, keyValueMapper, MockValueJoiner.STRING_JOINER, "view");
        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);
        driver.setTime(0);
        driver.process(globalTopic, "a", "A");
        driver.process(globalTopic, "b", "B");
        driver.process(global2Topic, "A", "1");
        driver.process(global2Topic, "B", "2");

        final ReadOnlyKeyValueStore<String, String> view = (ReadOnlyKeyValueStore<String, String>) driver.globalStateStore("view");
        assertEquals("A+1", view.get("a"));
        assertEquals("B+2", view.get("b"));
    }

    private void verifyGlobalGlobalJoin(final Map<String, String> expected, final String streamTopic) {
        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);
        driver.setTime(0L);
        driver.process(globalTopic,  "a", "A");
        driver.process(global2Topic, "A", "B");
        driver.process(streamTopic,  "1", "a");
        driver.process(streamTopic,  "2", "b");
        driver.flushState();
        assertEquals(expected, results);
    }

    private void verifyGlobalGlobalLeftJoin(final Map<String, String> expected, final String streamTopic) {
        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);
        driver.setTime(0L);
        driver.process(globalTopic,  "a", "A");
        driver.process(globalTopic,  "b", "B");
        driver.process(global2Topic, "A", "B");
        driver.process(streamTopic,  "1", "a");
        driver.process(streamTopic,  "2", "b");
        driver.flushState();
        assertEquals(expected, results);
    }

    private void verifyJoin(final Map<String, String> expected, final String joinInput) {
        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);
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
