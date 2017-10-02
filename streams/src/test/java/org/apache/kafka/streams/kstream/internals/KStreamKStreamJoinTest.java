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
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsBuilderTest;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
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

import static org.junit.Assert.assertEquals;

public class KStreamKStreamJoinTest {

    final private String topic1 = "topic1";
    final private String topic2 = "topic2";

    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();

    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();
    private File stateDir = null;
    private final Consumed<Integer, String> consumed = Consumed.with(intSerde, stringSerde);

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testJoin() {
        StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> joined;
        MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.join(stream2,
                              MockValueJoiner.TOSTRING_JOINER,
                              JoinWindows.of(100),
                              Joined.with(intSerde, stringSerde, stringSerde));
        joined.process(processor);

        Collection<Set<String>> copartitionGroups = StreamsBuilderTest.getCopartitionedGroups(builder);

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        driver.setUp(builder, stateDir);
        driver.setTime(0L);

        // push two items to the primary stream. the other window is empty
        // w1 = {}
        // w2 = {}
        // --> w1 = { 0:X0, 1:X1 }
        //     w2 = {}

        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }

        processor.checkAndClearProcessResult();

        // push two items to the other stream. this should produce two items.
        // w1 = { 0:X0, 1:X1 }
        // w2 = {}
        // --> w1 = { 0:X0, 1:X1 }
        //     w2 = { 0:Y0, 1:Y1 }

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }

        processor.checkAndClearProcessResult("0:X0+Y0", "1:X1+Y1");

        // push all four items to the primary stream. this should produce two items.
        // w1 = { 0:X0, 1:X1 }
        // w2 = { 0:Y0, 1:Y1 }
        // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
        //     w2 = { 0:Y0, 1:Y1 }

        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "X" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+Y0", "1:X1+Y1");

        // push all items to the other stream. this should produce six items.
        // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
        // w2 = { 0:Y0, 1:Y1 }
        // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
        //     w2 = { 0:Y0, 1:Y1, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }

        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "0:X0+YY0", "1:X1+YY1", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        // push all four items to the primary stream. this should produce six items.
        // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
        // w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3
        // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
        //     w2 = { 0:Y0, 1:Y1, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }

        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:XX0+Y0", "0:XX0+YY0", "1:XX1+Y1", "1:XX1+YY1", "2:XX2+YY2", "3:XX3+YY3");

        // push two items to the other stream. this should produce six item.
        // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
        // w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3
        // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
        //     w2 = { 0:Y0, 1:Y1, 0:YY0, 1:YY1, 2:YY2, 3:YY3, 0:YYY0, 1:YYY1 }

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "YYY" + expectedKeys[i]);
        }

        processor.checkAndClearProcessResult("0:X0+YYY0", "0:X0+YYY0", "0:XX0+YYY0", "1:X1+YYY1", "1:X1+YYY1", "1:XX1+YYY1");
    }

    @Test
    public void testOuterJoin() {
        StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> joined;
        MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);
        joined = stream1.outerJoin(stream2,
                                   MockValueJoiner.TOSTRING_JOINER,
                                   JoinWindows.of(100),
                                   Joined.with(intSerde, stringSerde, stringSerde));
        joined.process(processor);
        Collection<Set<String>> copartitionGroups = StreamsBuilderTest.getCopartitionedGroups(builder);

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        driver.setUp(builder, stateDir);
        driver.setTime(0L);

        // push two items to the primary stream. the other window is empty.this should produce two items
        // w1 = {}
        // w2 = {}
        // --> w1 = { 0:X0, 1:X1 }
        //     w2 = {}

        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }

        processor.checkAndClearProcessResult("0:X0+null", "1:X1+null");

        // push two items to the other stream. this should produce two items.
        // w1 = { 0:X0, 1:X1 }
        // w2 = {}
        // --> w1 = { 0:X0, 1:X1 }
        //     w2 = { 0:Y0, 1:Y1 }

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }

        processor.checkAndClearProcessResult("0:X0+Y0", "1:X1+Y1");

        // push all four items to the primary stream. this should produce four items.
        // w1 = { 0:X0, 1:X1 }
        // w2 = { 0:Y0, 1:Y1 }
        // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
        //     w2 = { 0:Y0, 1:Y1 }

        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "X" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+Y0", "1:X1+Y1", "2:X2+null", "3:X3+null");

        // push all items to the other stream. this should produce six items.
        // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
        // w2 = { 0:Y0, 1:Y1 }
        // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
        //     w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }

        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "0:X0+YY0", "1:X1+YY1", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        // push all four items to the primary stream. this should produce six items.
        // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3 }
        // w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3
        // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
        //     w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3 }

        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:XX0+Y0", "0:XX0+YY0", "1:XX1+Y1", "1:XX1+YY1", "2:XX2+YY2", "3:XX3+YY3");

        // push two items to the other stream. this should produce six item.
        // w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
        // w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3
        // --> w1 = { 0:X0, 1:X1, 0:X0, 1:X1, 2:X2, 3:X3,  0:XX0, 1:XX1, 2:XX2, 3:XX3 }
        //     w2 = { 0:Y0, 1:Y1, 0:YY0, 0:YY0, 1:YY1, 2:YY2, 3:YY3, 0:YYY0, 1:YYY1 }

        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "YYY" + expectedKeys[i]);
        }

        processor.checkAndClearProcessResult("0:X0+YYY0", "0:X0+YYY0", "0:XX0+YYY0", "1:X1+YYY1", "1:X1+YYY1", "1:XX1+YYY1");
    }

    @Test
    public void testWindowing() {
        long time = 0L;

        StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> joined;
        MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.join(stream2,
                              MockValueJoiner.TOSTRING_JOINER,
                              JoinWindows.of(100),
                              Joined.with(intSerde, stringSerde, stringSerde));
        joined.process(processor);

        Collection<Set<String>> copartitionGroups = StreamsBuilderTest.getCopartitionedGroups(builder);

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        driver.setUp(builder, stateDir);


        // push two items to the primary stream. the other window is empty. this should produce no items.
        // w1 = {}
        // w2 = {}
        // --> w1 = { 0:X0, 1:X1 }
        //     w2 = {}
        setRecordContext(time, topic1);
        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }

        processor.checkAndClearProcessResult();

        // push two items to the other stream. this should produce two items.
        // w1 = { 0:X0, 1:X1 }
        // w2 = {}
        // --> w1 = { 0:X0, 1:X1 }
        //     w2 = { 0:Y0, 1:Y1 }

        setRecordContext(time, topic2);
        for (int i = 0; i < 2; i++) {
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }

        processor.checkAndClearProcessResult("0:X0+Y0", "1:X1+Y1");

        // clear logically
        time = 1000L;
        setRecordContext(time, topic1);
        for (int i = 0; i < expectedKeys.length; i++) {
            setRecordContext(time + i, topic1);
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        processor.checkAndClearProcessResult();

        // gradually expires items in w1
        // w1 = { 0:X0, 1:X1, 2:X2, 3:X3 }

        time = 1000 + 100L;
        setRecordContext(time, topic2);

        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("2:X2+YY2", "3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult();

        // go back to the time before expiration

        time = 1000L - 100L - 1L;
        setRecordContext(time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult();

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        // clear (logically)
        time = 2000L;
        for (int i = 0; i < expectedKeys.length; i++) {
            setRecordContext(time + i, topic2);
            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
        }

        processor.checkAndClearProcessResult();

        // gradually expires items in w2
        // w2 = { 0:Y0, 1:Y1, 2:Y2, 3:Y3 }

        time = 2000L + 100L;
        setRecordContext(time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("1:XX1+Y1", "2:XX2+Y2", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("2:XX2+Y2", "3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("3:XX3+Y3");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult();

        // go back to the time before expiration

        time = 2000L - 100L - 1L;
        setRecordContext(time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult();

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:XX0+Y0");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+Y1");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+Y1", "2:XX2+Y2");

        setRecordContext(++time, topic1);
        for (int expectedKey : expectedKeys) {
            driver.process(topic1, expectedKey, "XX" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:XX0+Y0", "1:XX1+Y1", "2:XX2+Y2", "3:XX3+Y3");
    }

    @Test
    public void testAsymetricWindowingAfter() {
        long time = 1000L;

        StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> joined;
        MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.join(stream2,
                              MockValueJoiner.TOSTRING_JOINER,
                              JoinWindows.of(0).after(100),
                              Joined.with(intSerde,
                                          stringSerde,
                                          stringSerde));
        joined.process(processor);

        Collection<Set<String>> copartitionGroups = StreamsBuilderTest.getCopartitionedGroups(builder);

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        driver.setUp(builder, stateDir);

        for (int i = 0; i < expectedKeys.length; i++) {
            setRecordContext(time + i, topic1);
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        processor.checkAndClearProcessResult();


        time = 1000L - 1L;
        setRecordContext(time, topic2);

        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult();

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        time = 1000 + 100L;
        setRecordContext(time, topic2);

        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("2:X2+YY2", "3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult();
    }

    @Test
    public void testAsymetricWindowingBefore() {
        long time = 1000L;

        StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream1;
        KStream<Integer, String> stream2;
        KStream<Integer, String> joined;
        MockProcessorSupplier<Integer, String> processor;

        processor = new MockProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.join(stream2,
                              MockValueJoiner.TOSTRING_JOINER,
                              JoinWindows.of(0).before(100),
                              Joined.with(intSerde, stringSerde, stringSerde));
        joined.process(processor);

        Collection<Set<String>> copartitionGroups = StreamsBuilderTest.getCopartitionedGroups(builder);

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        driver.setUp(builder, stateDir);

        for (int i = 0; i < expectedKeys.length; i++) {
            setRecordContext(time + i, topic1);
            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
        }
        processor.checkAndClearProcessResult();


        time = 1000L - 100L - 1L;

        setRecordContext(time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult();

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");
        time = 1000L;

        setRecordContext(time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("1:X1+YY1", "2:X2+YY2", "3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("2:X2+YY2", "3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult("3:X3+YY3");

        setRecordContext(++time, topic2);
        for (int expectedKey : expectedKeys) {
            driver.process(topic2, expectedKey, "YY" + expectedKey);
        }

        processor.checkAndClearProcessResult();
    }

    private void setRecordContext(final long time, final String topic) {
        ((MockProcessorContext) driver.context()).setRecordContext(new ProcessorRecordContext(time, 0, 0, topic));
    }
}
