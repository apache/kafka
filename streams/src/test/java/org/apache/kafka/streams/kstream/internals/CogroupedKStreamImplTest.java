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

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CogroupedKStreamImplTest {

    private static final String TOPIC_1 = "topic-1";
    private static final String TOPIC_2 = "topic-2";
    private static final String INVALID_STORE_NAME = "~foo bar~";
    private final KStreamBuilder builder = new KStreamBuilder();
    private CogroupedKStream<String, String> cogroupedStream;
    private KStreamTestDriver driver = null;
    
    private final Merger<String, String> sessionMerger = new Merger<String, String>() {
        @Override
        public String apply(final String aggKey, final String aggOne, final String aggTwo) {
            return null;
        }
    };

    @Before
    public void before() {
        final KStream<String, String> stream = builder.stream(Serdes.String(), Serdes.String(), TOPIC_1);
        cogroupedStream = stream.groupByKey(Serdes.String(), Serdes.String()).cogroup(MockAggregator.TOSTRING_ADDER);
    }

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullKGroupedStreamOnCogroup() throws Exception {
        cogroupedStream.cogroup(null, MockAggregator.TOSTRING_ADDER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAggregatorOnCogroup() throws Exception {
        cogroupedStream.cogroup(builder.stream(Serdes.String(), Serdes.String(), TOPIC_2).groupByKey(), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() throws Exception {
        cogroupedStream.aggregate(null, Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreNameOnAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, Serdes.String(), null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, Serdes.String(), INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(null, TimeWindows.of(10), Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsOnWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, null, Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreNameOnWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, TimeWindows.of(10), Serdes.String(), null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, TimeWindows.of(10), Serdes.String(), INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, TimeWindows.of(10), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnSessionWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(null, sessionMerger, SessionWindows.with(10), Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullSessionMergerOnSessionWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, null, SessionWindows.with(10), Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsOnSessionWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, sessionMerger, null, Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreNameOnSessionWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, sessionMerger, SessionWindows.with(10), Serdes.String(), null);
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldNotHaveInvalidStoreNameOnSessionWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, sessionMerger, SessionWindows.with(10), Serdes.String(), INVALID_STORE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreSupplierOnSessionWindowedAggregate() throws Exception {
        cogroupedStream.aggregate(MockInitializer.STRING_INIT, sessionMerger, SessionWindows.with(10), null);
    }
}
