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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WindowKeySchemaTest {

    private final WindowKeySchema windowKeySchema = new WindowKeySchema();
    private DelegatingPeekingKeyValueIterator<Bytes, Integer> iterator;

    @Before
    public void before() {
        windowKeySchema.init("topic");
        final List<KeyValue<Bytes, Integer>> keys = Arrays.asList(KeyValue.pair(Bytes.wrap(WindowKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new TimeWindow(0, 0)))), 1),
                                                                  KeyValue.pair(Bytes.wrap(WindowKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new TimeWindow(0, 0)))), 2),
                                                                  KeyValue.pair(Bytes.wrap(WindowKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new TimeWindow(0, 0)))), 3),
                                                                  KeyValue.pair(Bytes.wrap(WindowKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new TimeWindow(10, 20)))), 4),
                                                                  KeyValue.pair(Bytes.wrap(WindowKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new TimeWindow(10, 20)))), 5),
                                                                  KeyValue.pair(Bytes.wrap(WindowKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new TimeWindow(10, 20)))), 6));
        iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()));
    }
    
    @Test
    public void testHasNextConditionUsingNullKeys() {
        final HasNextCondition hasNextCondition = windowKeySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE);
        final List<Integer> results = getValues(hasNextCondition);
        assertThat(results, equalTo(Arrays.asList(1, 2, 3, 4, 5, 6)));
    }

    @Test
    public void testUpperBoundWithLargeTimestamps() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                    WindowKeySchema.toStoreKeyBinary(
                    new byte[]{0xA},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                    WindowKeySchema.toStoreKeyBinary(
                    new byte[]{0xA, 0xB},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(upper, equalTo(WindowKeySchema.toStoreKeyBinary(new byte[]{0xA}, Long.MAX_VALUE, Integer.MAX_VALUE)));
    }

    @Test
    public void testUpperBoundWithKeyBytesLargerThanFirstTimestampByte() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                    WindowKeySchema.toStoreKeyBinary(
                    new byte[]{0xA, (byte) 0x8F},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(upper, equalTo(WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}, Long.MAX_VALUE, Integer.MAX_VALUE)));
    }


    @Test
    public void testUpperBoundWithKeyBytesLargerAndSmallerThanFirstTimestampByte() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xC, 0xC, 0x9}), 0x0AffffffffffffffL);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                    WindowKeySchema.toStoreKeyBinary(
                    new byte[]{0xC, 0xC},
                    0x0AffffffffffffffL,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(upper, equalTo(WindowKeySchema.toStoreKeyBinary(new byte[]{0xC, 0xC}, 0x0AffffffffffffffL, Integer.MAX_VALUE)));
    }

    @Test
    public void testUpperBoundWithZeroTimestamp() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        assertThat(upper, equalTo(WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, 0, Integer.MAX_VALUE)));
    }

    @Test
    public void testLowerBoundWithZeroTimestamp() {
        Bytes lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        assertThat(lower, equalTo(WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, 0, 0)));
    }

    @Test
    public void testLowerBoundWithMonZeroTimestamp() {
        Bytes lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 42);
        assertThat(lower, equalTo(WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, 0, 0)));
    }

    @Test
    public void testLowerBoundMatchesTrailingZeros() {
        Bytes lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE - 1);

        assertThat(
            "appending zeros to key should still be in range",
            lower.compareTo(
                    WindowKeySchema.toStoreKeyBinary(
                        new byte[]{0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                        Long.MAX_VALUE - 1,
                        0
                )
            ) < 0
        );

        assertThat(lower, equalTo(WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, 0, 0)));
    }
    
    private List<Integer> getValues(final HasNextCondition hasNextCondition) {
        final List<Integer> results = new ArrayList<>();
        while (hasNextCondition.hasNext(iterator)) {
            results.add(iterator.next().value);
        }
        return results;
    }
}
