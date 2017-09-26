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
package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class StreamsBuilderTest {

    private final StreamsBuilder builder = new StreamsBuilder();

    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Test(expected = TopologyException.class)
    public void testFrom() {
        builder.stream(Arrays.asList("topic-1", "topic-2"));

        builder.build().addSource(KStreamImpl.SOURCE_NAME + "0000000000", "topic-3");
    }

    @Test
    public void shouldProcessingFromSinkTopic() {
        final KStream<String, String> source = builder.stream("topic-source");
        source.to("topic-sink");

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();

        source.process(processorSupplier);

        driver.setUp(builder);
        driver.setTime(0L);

        driver.process("topic-source", "A", "aa");

        // no exception was thrown
        assertEquals(Utils.mkList("A:aa"), processorSupplier.processed);
    }

    @Test
    public void shouldProcessViaThroughTopic() {
        final KStream<String, String> source = builder.stream("topic-source");
        final KStream<String, String> through = source.through("topic-sink");

        final MockProcessorSupplier<String, String> sourceProcessorSupplier = new MockProcessorSupplier<>();
        final MockProcessorSupplier<String, String> throughProcessorSupplier = new MockProcessorSupplier<>();

        source.process(sourceProcessorSupplier);
        through.process(throughProcessorSupplier);

        driver.setUp(builder);
        driver.setTime(0L);

        driver.process("topic-source", "A", "aa");

        assertEquals(Utils.mkList("A:aa"), sourceProcessorSupplier.processed);
        assertEquals(Utils.mkList("A:aa"), throughProcessorSupplier.processed);
    }
    
    @Test
    public void testMerge() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> merged = source1.merge(source2);

        final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        merged.process(processorSupplier);

        driver.setUp(builder);
        driver.setTime(0L);

        driver.process(topic1, "A", "aa");
        driver.process(topic2, "B", "bb");
        driver.process(topic2, "C", "cc");
        driver.process(topic1, "D", "dd");

        assertEquals(Utils.mkList("A:aa", "B:bb", "C:cc", "D:dd"), processorSupplier.processed);
    }

    @Test
    public void shouldUseSerdesDefinedInMaterializedToConsumeTable() {
        final Map<Long, String> results = new HashMap<>();
        final String topic = "topic";
        final ForeachAction<Long, String> action = new ForeachAction<Long, String>() {
            @Override
            public void apply(final Long key, final String value) {
                results.put(key, value);
            }
        };
        builder.table(topic, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("store")
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.String()))
                .toStream().foreach(action);

        driver.setUp(builder, TestUtils.tempDirectory());
        driver.setTime(0L);
        driver.process(topic, 1L, "value1");
        driver.process(topic, 2L, "value2");
        driver.flushState();
        final KeyValueStore<Long, String> store = (KeyValueStore) driver.allStateStores().get("store");
        assertThat(store.get(1L), equalTo("value1"));
        assertThat(store.get(2L), equalTo("value2"));
        assertThat(results.get(1L), equalTo("value1"));
        assertThat(results.get(2L), equalTo("value2"));
    }

    @Test
    public void shouldUseSerdesDefinedInMaterializedToConsumeGlobalTable() {
        final String topic = "topic";
        builder.globalTable(topic, Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as("store")
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.String()));
        driver.setUp(builder, TestUtils.tempDirectory());
        driver.setTime(0L);
        driver.process(topic, 1L, "value1");
        driver.process(topic, 2L, "value2");
        driver.flushState();
        final KeyValueStore<Long, String> store = (KeyValueStore) driver.allStateStores().get("store");
        assertThat(store.get(1L), equalTo("value1"));
        assertThat(store.get(2L), equalTo("value2"));
    }
    
    @Test(expected = TopologyException.class)
    public void shouldThrowExceptionWhenNoTopicPresent() throws Exception {
        builder.stream(Collections.<String>emptyList());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenTopicNamesAreNull() throws Exception {
        builder.stream(Arrays.<String>asList(null, null));
    }

    // TODO: these two static functions are added because some non-TopologyBuilder unit tests need to access the internal topology builder,
    //       which is usually a bad sign of design patterns between TopologyBuilder and StreamThread. We need to consider getting rid of them later
    public static InternalTopologyBuilder internalTopologyBuilder(final StreamsBuilder builder) {
        return builder.internalTopologyBuilder;
    }

    public static Collection<Set<String>> getCopartitionedGroups(final StreamsBuilder builder) {
        return builder.internalTopologyBuilder.copartitionGroups();
    }
}