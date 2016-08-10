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
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.junit.Before;
import org.junit.Test;

public class KGroupedStreamImplTest {

    private KGroupedStream<String, String> groupedStream;

    @Before
    public void before() {
        final KStream<String, String> stream = new KStreamBuilder().stream("topic");
        groupedStream = stream.groupByKey();
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerOnReduce() throws Exception {
        groupedStream.reduce(null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreNameOnReduce() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullReducerWithWindowedReduce() throws Exception {
        groupedStream.reduce(null, TimeWindows.of(10), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsWithWindowedReduce() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreNameWithWindowedReduce() throws Exception {
        groupedStream.reduce(MockReducer.STRING_ADDER, TimeWindows.of(10), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() throws Exception {
        groupedStream.aggregate(null, MockAggregator.STRING_ADDER, Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreNameOnAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.STRING_ADDER, Serdes.String(), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(null, MockAggregator.STRING_ADDER, TimeWindows.of(10), Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullAdderOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, null, TimeWindows.of(10), Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullWindowsOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.STRING_ADDER, null, Serdes.String(), "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullStoreNameOnWindowedAggregate() throws Exception {
        groupedStream.aggregate(MockInitializer.STRING_INIT, MockAggregator.STRING_ADDER, TimeWindows.of(10), Serdes.String(), null);
    }
}