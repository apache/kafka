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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

@Deprecated
public class KTableForeachTest {

    final private String topicName = "topic";
    private File stateDir = null;
    final private Serde<Integer> intSerde = Serdes.Integer();
    final private Serde<String> stringSerde = Serdes.String();
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Before
    public void setUp() {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testForeach() {
        // Given
        List<KeyValue<Integer, String>> inputRecords = Arrays.asList(
            new KeyValue<>(0, "zero"),
            new KeyValue<>(1, "one"),
            new KeyValue<>(2, "two"),
            new KeyValue<>(3, "three")
        );

        List<KeyValue<Integer, String>> expectedRecords = Arrays.asList(
            new KeyValue<>(0, "ZERO"),
            new KeyValue<>(2, "ONE"),
            new KeyValue<>(4, "TWO"),
            new KeyValue<>(6, "THREE")
        );

        final List<KeyValue<Integer, String>> actualRecords = new ArrayList<>();
        ForeachAction<Integer, String> action =
            new ForeachAction<Integer, String>() {
                @Override
                public void apply(Integer key, String value) {
                    actualRecords.add(new KeyValue<>(key * 2, value.toUpperCase(Locale.ROOT)));
                }
            };

        // When
        StreamsBuilder builder = new StreamsBuilder();
        KTable<Integer, String> table = builder.table(topicName,
                                                      Consumed.with(intSerde, stringSerde),
                                                      Materialized.<Integer, String, KeyValueStore<Bytes, byte[]>>as(topicName)
                                                              .withKeySerde(intSerde)
                                                              .withValueSerde(stringSerde));
        table.foreach(action);

        // Then
        driver.setUp(builder, stateDir);
        for (KeyValue<Integer, String> record: inputRecords) {
            driver.process(topicName, record.key, record.value);
        }
        driver.flushState();

        assertEquals(expectedRecords.size(), actualRecords.size());
        for (int i = 0; i < expectedRecords.size(); i++) {
            KeyValue<Integer, String> expectedRecord = expectedRecords.get(i);
            KeyValue<Integer, String> actualRecord = actualRecords.get(i);
            assertEquals(expectedRecord, actualRecord);
        }
    }

    @Test
    public void testTypeVariance() {
        ForeachAction<Number, Object> consume = new ForeachAction<Number, Object>() {
            @Override
            public void apply(Number key, Object value) {}
        };

        new StreamsBuilder()
            .<Integer, String>table("emptyTopic")
            .foreach(consume);
    }
}
