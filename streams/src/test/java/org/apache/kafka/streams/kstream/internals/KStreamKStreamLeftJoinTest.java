/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class KStreamKStreamLeftJoinTest {

    final private String topic1 = "topic1";
    final private String topic2 = "topic2";

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();

    private KStreamTestDriver driver = null;
    private File stateDir = null;

    @After
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }


    @Test
    public void testLeftJoin() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> joined;
        MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream1 = builder.stream(intSerde, stringSerde, topic1);
        stream2 = builder.stream(intSerde, stringSerde, topic2);

        joined = stream1.leftJoin(stream2, MockValueJoiner.STRING_JOINER, JoinWindows.of(100), intSerde, stringSerde, stringSerde);
        joined.process(processor);

        Collection<Set<String>> copartitionGroups = builder.copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        driver = new KStreamTestDriver(builder, stateDir);
        driver.setTime(0L);

        // push two items to the primary stream. the other window is empty
        // w {}
        // --> w = {}

        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:X0+null", "1:X1+null");

        // push two items to the other stream. this should produce two items.
        // w {}
        // --> w = { 0:Y0, 1:Y1 }

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult();

        // push all four items to the primary stream. this should produce four items.
        // w = { 0:Y0, 1:Y1 }
        // --> w = { 0:Y0, 1:Y1 }

        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:X0+Y0", "1:X1+Y1", "2:X2+null", "3:X3+null");

        // push all items to the other stream. this should produce no items.
        // w = { 0:Y0, 1:Y1 }
        // --> w = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }

        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic2, expectedKeys[i], "YY" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult();

        // push all four items to the primary stream. this should produce four items.
        // w = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3
        // --> w = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }

        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+Y0", "0:XX0+YY0", "1:XX1+Y1", "1:XX1+YY1", "2:XX2+YY2", "3:XX3+YY3");
    }

    @Test
    public void testWindowing() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        long time = 0L;

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> joined;
        MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream1 = builder.stream(intSerde, stringSerde, topic1);
        stream2 = builder.stream(intSerde, stringSerde, topic2);

        joined = stream1.leftJoin(stream2, MockValueJoiner.STRING_JOINER, JoinWindows.of(100), intSerde, stringSerde, stringSerde);
        joined.process(processor);

        Collection<Set<String>> copartitionGroups = builder.copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        driver = new KStreamTestDriver(builder, stateDir);

        // push two items to the primary stream. the other window is empty. this should produce two items
        // w = {}
        // --> w = {}

        setRecordContext(time, topic1);
        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:X0+null", "1:X1+null");

        // push two items to the other stream. this should produce no items.
        // w = {}
        // --> w = { 0:Y0, 1:Y1 }

        setRecordContext(time, topic2);
        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult();

        // clear logically
        time = 1000L;
        setRecordContext(time, topic2);

        // push all items to the other stream. this should produce no items.
        // w = {}
        // --> w = { 0:Y0, 1:Y1, 2:Y2, 3:Y3 }
        for (int i = 0; i < expectedKeys.length; i++) {
            setRecordContext(time + i, topic2);
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult();

        // gradually expire items in window.
        // w = { 0:Y0, 1:Y1, 2:Y2, 3:Y3 }

        time = 1000L + 100L;
        setRecordContext(time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+null", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+null", "1:XX1+null", "2:XX2+Y2", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+null", "1:XX1+null", "2:XX2+null", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+null", "1:XX1+null", "2:XX2+null", "3:XX3+null");

        // go back to the time before expiration

        time = 1000L - 100L - 1L;
        setRecordContext(time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+null", "1:XX1+null", "2:XX2+null", "3:XX3+null");

        setRecordContext(++time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+null", "2:XX2+null", "3:XX3+null");

        setRecordContext(++time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+Y1", "2:XX2+null", "3:XX3+null");

        setRecordContext(++time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+null");

        setRecordContext(++time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
        }
        driver.flushState();
        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+Y3");
    }

    private void setRecordContext(final long time, final String topic) {
        ((MockProcessorContext) driver.context()).setRecordContext(new ProcessorRecordContext(time, 0, 0, topic));
    }
}
