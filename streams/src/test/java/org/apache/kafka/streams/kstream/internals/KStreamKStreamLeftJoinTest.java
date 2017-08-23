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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsBuilderTest;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.KStreamKStreamJoinTest.checkResult;
import static org.junit.Assert.assertEquals;

public class KStreamKStreamLeftJoinTest {

    final private String topic1 = "topic1";
    final private String topic2 = "topic2";

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();
    private File stateDir = null;


    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    private void testLeftJoin(boolean withKey) {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream1 = builder.stream(intSerde, stringSerde, topic1);
        stream2 = builder.stream(intSerde, stringSerde, topic2);

        if (withKey) {
            joined = stream1.leftJoin(stream2, MockValueJoiner.TOSTRING_JOINER_WITH_KEY, JoinWindows.of(100), intSerde, stringSerde, stringSerde);
        } else {
            joined = stream1.leftJoin(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(100), intSerde, stringSerde, stringSerde);
        }
        joined.process(processor);

        final Collection<Set<String>> copartitionGroups = StreamsBuilderTest.getCopartitionedGroups(builder);

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        driver.setUp(builder, stateDir);
        driver.setTime(0L);

        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:X0+null", "1:X1+null");

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:X0+Y0", "1:X1+Y1");

        for (int i = 0; i < 3; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:X0+Y0", "1:X1+Y1", "2:X2+null");

        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:X0+YY0", "0:X0+YY0", "1:X1+YY1", "1:X1+YY1", "2:X2+YY2");

        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+Y0", "0:XX0+YY0", "1:XX1+Y1", "1:XX1+YY1", "2:XX2+YY2", "3:XX3+YY3");
    }

    private void testWindowing(boolean withKey) {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        long time = 0L;

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream1 = builder.stream(intSerde, stringSerde, topic1);
        stream2 = builder.stream(intSerde, stringSerde, topic2);

        if (withKey) {
            joined = stream1.leftJoin(stream2, MockValueJoiner.TOSTRING_JOINER_WITH_KEY, JoinWindows.of(100), intSerde, stringSerde, stringSerde);
        } else {
            joined = stream1.leftJoin(stream2, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(100), intSerde, stringSerde, stringSerde);
        }
        joined.process(processor);

        final Collection<Set<String>> copartitionGroups = StreamsBuilderTest.getCopartitionedGroups(builder);

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        driver.setUp(builder, stateDir);

        setRecordContext(time, topic1);
        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:X0+null", "1:X1+null");

        setRecordContext(time, topic2);
        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:X0+Y0", "1:X1+Y1");

        // clear logically
        time = 1000L;
        setRecordContext(time, topic2);

        for (int i = 0; i < expectedKeys.length; i++) {
            setRecordContext(time + i, topic2);
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult();

        time = 1000L + 100L;
        setRecordContext(time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+Y0", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+null", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+null", "1:XX1+null", "2:XX2+Y2", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+null", "1:XX1+null", "2:XX2+null", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+null", "1:XX1+null", "2:XX2+null", "3:XX3+null");
        // go back to the time before expiration

        time = 1000L - 100L - 1L;
        setRecordContext(time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+null", "1:XX1+null", "2:XX2+null", "3:XX3+null");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+Y0", "1:XX1+null", "2:XX2+null", "3:XX3+null");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+Y0", "1:XX1+Y1", "2:XX2+null", "3:XX3+null");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+Y0", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+null");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }
        driver.flushState();

        checkResult(processor, withKey, "0:XX0+Y0", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+Y3");
    }

    @Test
    public void testLeftJoin() throws Exception {
        testLeftJoin(false);
    }

    @Test
    public void testLeftJoinWithKey() throws Exception {
        testLeftJoin(true);
    }

    @Test
    public void testWindowing() throws Exception {
        testWindowing(false);
    }

    @Test
    public void testWindowingWithKey() throws Exception {
        testWindowing(true);
    }

    private void setRecordContext(final long time, final String topic) {
        ((MockProcessorContext) driver.context()).setRecordContext(new ProcessorRecordContext(time, 0, 0, topic));
    }
}
