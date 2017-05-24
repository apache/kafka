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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Test;

import static org.junit.Assert.fail;

public class KTableReduceTest {

    @Test
    public void shouldNotSubtractOldValueOnlyIfStateIsNull() {
        final Processor<Long, Change<Long>> reducer = new KTableReduce<Long, Long>(
            "corruptedStore",
            new Reducer<Long>() {
                @Override
                public Long apply(final Long value1, final Long value2) {
                    throw new RuntimeException("Subtractro#apply() should never be called for subtraction case.");
                }
            },
            new Reducer<Long>() {
                @Override
                public Long apply(final Long value1, final Long value2) {
                    throw new RuntimeException("Adder#apply() should never be called for subtraction case.");
                }
            }
        ).get();

        reducer.init(
            new MockProcessorContext(
                StateSerdes.withBuiltinTypes("anyName", Long.class, Long.class),
                new RecordCollectorImpl(
                    new MockProducer<>(true, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer()),
                    null)) {

                @Override
                public StateStore getStateStore(final String storeName) {
                    return new InMemoryKeyValueStore<>(
                        "corruptedStore",
                        Serdes.Long(),
                        Serdes.Long());
                }

                @SuppressWarnings("unchecked")
                @Override
                public void forward(final Object key, final Object value) {
                    fail("Should not forward anything for subtraction on empty state.");
                }
            });

        reducer.process(21L, new Change<>(null, 42L));
    }

}
