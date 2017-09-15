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
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class SessionKeySchemaTest {

    private final SessionKeySchema sessionKeySchema = new SessionKeySchema();
    private DelegatingPeekingKeyValueIterator<Bytes, Integer> iterator;

    @Before
    public void before() {
        sessionKeySchema.init("topic");
        final List<KeyValue<Bytes, Integer>> keys = Arrays.asList(KeyValue.pair(SessionKeySerde.bytesToBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new SessionWindow(0, 0))), 1),
                                                                  KeyValue.pair(SessionKeySerde.bytesToBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new SessionWindow(0, 0))), 2),
                                                                  KeyValue.pair(SessionKeySerde.bytesToBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new SessionWindow(0, 0))), 3),
                                                                  KeyValue.pair(SessionKeySerde.bytesToBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new SessionWindow(10, 20))), 4),
                                                                  KeyValue.pair(SessionKeySerde.bytesToBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new SessionWindow(10, 20))), 5),
                                                                  KeyValue.pair(SessionKeySerde.bytesToBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new SessionWindow(10, 20))), 6));
        iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()));
    }

    @Test
    public void shouldFetchExactKeysSkippingLongerKeys() {
        final Bytes key = Bytes.wrap(new byte[]{0});
        final List<Integer> result = getValues(sessionKeySchema.hasNextCondition(key, key, 0, Long.MAX_VALUE));
        assertThat(result, equalTo(Arrays.asList(2, 4)));
    }

    @Test
    public void shouldFetchExactKeySkippingShorterKeys() {
        final Bytes key = Bytes.wrap(new byte[]{0, 0});
        final HasNextCondition hasNextCondition = sessionKeySchema.hasNextCondition(key, key, 0, Long.MAX_VALUE);
        final List<Integer> results = getValues(hasNextCondition);
        assertThat(results, equalTo(Arrays.asList(1, 5)));
    }

    @Test
    public void testUpperBoundWithLargeTimestamps() {
        Bytes upper = sessionKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                SessionKeySerde.bytesToBinary(
                    new Windowed<>(
                        Bytes.wrap(new byte[]{0xA}),
                        new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
                )
            ) >= 0
        );

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                SessionKeySerde.bytesToBinary(
                    new Windowed<>(
                        Bytes.wrap(new byte[]{0xA, 0xB}),
                        new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
                )
            ) >= 0
        );

        assertThat(upper, equalTo(SessionKeySerde.bytesToBinary(
            new Windowed<>(Bytes.wrap(new byte[]{0xA}), new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))))
        );
    }

    @Test
    public void testUpperBoundWithKeyBytesLargerThanFirstTimestampByte() {
        Bytes upper = sessionKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                SessionKeySerde.bytesToBinary(
                    new Windowed<>(
                        Bytes.wrap(new byte[]{0xA, (byte) 0x8F}),
                        new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
                )
            ) >= 0
        );

        assertThat(upper, equalTo(SessionKeySerde.bytesToBinary(
            new Windowed<>(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))))
        );
    }

    @Test
    public void testUpperBoundWithZeroTimestamp() {
        Bytes upper = sessionKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);

        assertThat(upper, equalTo(SessionKeySerde.bytesToBinary(
            new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, 0))))
        );
    }

    @Test
    public void testLowerBoundWithZeroTimestamp() {
        Bytes lower = sessionKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        assertThat(lower, equalTo(SessionKeySerde.bytesToBinary(new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, 0)))));
    }

    @Test
    public void testLowerBoundMatchesTrailingZeros() {
        Bytes lower = sessionKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
            "appending zeros to key should still be in range",
            lower.compareTo(
                SessionKeySerde.bytesToBinary(
                    new Windowed<>(
                        Bytes.wrap(new byte[]{0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                        new SessionWindow(Long.MAX_VALUE, Long.MAX_VALUE))
                )
            ) < 0
        );

        assertThat(lower, equalTo(SessionKeySerde.bytesToBinary(new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, 0)))));
    }


    private List<Integer> getValues(final HasNextCondition hasNextCondition) {
        final List<Integer> results = new ArrayList<>();
        while (hasNextCondition.hasNext(iterator)) {
            results.add(iterator.next().value);
        }
        return results;
    }

}
