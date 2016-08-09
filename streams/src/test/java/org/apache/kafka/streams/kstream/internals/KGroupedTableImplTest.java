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
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockReducer;
import org.junit.Before;
import org.junit.Test;


public class KGroupedTableImplTest {

    private KGroupedTable<String, String> groupedTable;

    @Before
    public void before() {
        final KStreamBuilder builder = new KStreamBuilder();
        groupedTable = builder.table(Serdes.String(), Serdes.String(), "blah", "blah")
                .groupBy(MockKeyValueMapper.<String, String>SelectValueKeyValueMapper());
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStoreNameOnAggregate() throws Exception {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.STRING_ADDER, MockAggregator.STRING_REMOVER, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullInitializerOnAggregate() throws Exception {
        groupedTable.aggregate(null, MockAggregator.STRING_ADDER, MockAggregator.STRING_REMOVER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullAdderOnAggregate() throws Exception {
        groupedTable.aggregate(MockInitializer.STRING_INIT, null, MockAggregator.STRING_REMOVER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSubtractorOnAggregate() throws Exception {
        groupedTable.aggregate(MockInitializer.STRING_INIT, MockAggregator.STRING_ADDER, null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullAdderOnReduce() throws Exception {
        groupedTable.reduce(null, MockReducer.STRING_REMOVER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSubtractorOnReduce() throws Exception {
        groupedTable.reduce(MockReducer.STRING_ADDER, null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullStoreNameOnReduce() throws Exception {
        groupedTable.reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, null);
    }
}