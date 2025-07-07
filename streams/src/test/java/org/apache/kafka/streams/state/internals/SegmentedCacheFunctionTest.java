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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.apache.kafka.streams.state.StateSerdes.TIMESTAMP_SIZE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

class SegmentedCacheFunctionTest {

    private static final int SEGMENT_INTERVAL = 17;
    private static final int START_TIMESTAMP = 736213517;
    private static final int END_TIMESTAMP = 800000000;

    private static final Bytes THE_WINDOW_KEY = WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, START_TIMESTAMP, 42);
    private static final Bytes THE_SESSION_KEY = toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, END_TIMESTAMP, START_TIMESTAMP);

    private static final Bytes THE_WINDOW_CACHE_KEY = Bytes.wrap(
        ByteBuffer.allocate(8 + THE_WINDOW_KEY.get().length)
            .putLong(START_TIMESTAMP / SEGMENT_INTERVAL)
            .put(THE_WINDOW_KEY.get()).array()
    );

    private static final Bytes THE_SESSION_CACHE_KEY = Bytes.wrap(
        ByteBuffer.allocate(8 + THE_SESSION_KEY.get().length)
                .putLong(END_TIMESTAMP / SEGMENT_INTERVAL)
                .put(THE_SESSION_KEY.get()).array()
    );
    
    private SegmentedCacheFunction createCacheFunction(final SegmentedBytesStore.KeySchema keySchema) {
        return new SegmentedCacheFunction(keySchema, SEGMENT_INTERVAL);
    }

    private static Stream<Arguments> provideKeysAndSchemas() {
        return Stream.of(
                Arguments.of(THE_WINDOW_CACHE_KEY, THE_WINDOW_KEY, new WindowKeySchema()),
                Arguments.of(THE_SESSION_CACHE_KEY, THE_SESSION_KEY, new SessionKeySchema())
        );
    }

    private static Stream<Arguments> provideKeysTimestampsAndSchemas() {
        return Stream.of(
                Arguments.of(THE_WINDOW_KEY, START_TIMESTAMP, new WindowKeySchema()),
                Arguments.of(THE_SESSION_KEY, END_TIMESTAMP, new SessionKeySchema())
        );
    }

    private static Stream<Arguments> provideKeysForBoundaryChecks() {
        final Bytes sameKeyInPriorSegmentWindow = WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, 1234, 42);
        final Bytes sameKeyInPriorSegmentSession = toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, 1234, 12345);

        final Bytes lowerKeyInSameSegmentWindow = WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xB}, START_TIMESTAMP - 1, 0);
        final Bytes lowerKeyInSameSegmentSession = toStoreKeyBinary(new byte[]{0xA, 0xB, 0xB}, END_TIMESTAMP - 1, START_TIMESTAMP + 1);
        
        return Stream.of(
                Arguments.of(THE_WINDOW_KEY, new WindowKeySchema(), sameKeyInPriorSegmentWindow, lowerKeyInSameSegmentWindow),
                Arguments.of(THE_SESSION_KEY, new SessionKeySchema(), sameKeyInPriorSegmentSession, lowerKeyInSameSegmentSession)
        );
    }

    static Bytes toStoreKeyBinary(final byte[] serializedKey,
                                  final long endTime,
                                  final long startTime) {
        final ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + TIMESTAMP_SIZE + TIMESTAMP_SIZE);
        buf.put(serializedKey);
        buf.putLong(endTime);
        buf.putLong(startTime);

        return Bytes.wrap(buf.array());
    }

    @ParameterizedTest
    @MethodSource("provideKeysAndSchemas")
    void testKey(final Bytes cacheKey, final Bytes key, final SegmentedBytesStore.KeySchema keySchema) {
        assertThat(
                createCacheFunction(keySchema).key(cacheKey),
                equalTo(key)
        );
    }

    @ParameterizedTest
    @MethodSource("provideKeysTimestampsAndSchemas")
    void cacheKey(final Bytes key, final int timeStamp, final SegmentedBytesStore.KeySchema keySchema) {
        final long segmentId = timeStamp / SEGMENT_INTERVAL;
        final Bytes actualCacheKey = createCacheFunction(keySchema).cacheKey(key);
        final ByteBuffer buffer = ByteBuffer.wrap(actualCacheKey.get());

        assertThat(buffer.getLong(), equalTo(segmentId));

        final byte[] actualKey = new byte[buffer.remaining()];
        buffer.get(actualKey);
        assertThat(Bytes.wrap(actualKey), equalTo(key));
    }

    @ParameterizedTest
    @MethodSource("provideKeysAndSchemas")
    void testRoundTripping(final Bytes cacheKey, final Bytes key, final SegmentedBytesStore.KeySchema keySchema) {
        final SegmentedCacheFunction cacheFunction = createCacheFunction(keySchema);

        assertThat(
            cacheFunction.key(cacheFunction.cacheKey(key)),
            equalTo(key)
        );

        assertThat(
            cacheFunction.cacheKey(cacheFunction.key(cacheKey)),
            equalTo(cacheKey)
        );
    }

    @ParameterizedTest
    @MethodSource("provideKeysForBoundaryChecks")
    void compareSegmentedKeys(final Bytes key, final SegmentedBytesStore.KeySchema keySchema, final Bytes sameKeyInPriorSegment, final Bytes lowerKeyInSameSegment) {
        final SegmentedCacheFunction cacheFunction = createCacheFunction(keySchema);
        assertThat(
            "same key in same segment should be ranked the same",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(key),
                key
            ) == 0
        );

        assertThat(
            "same keys in different segments should be ordered according to segment",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(sameKeyInPriorSegment),
                key
            ) < 0
        );

        assertThat(
            "same keys in different segments should be ordered according to segment",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(key),
                sameKeyInPriorSegment
            ) > 0
        );

        assertThat(
            "different keys in same segments should be ordered according to key",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(key),
                lowerKeyInSameSegment
            ) > 0
        );

        assertThat(
            "different keys in same segments should be ordered according to key",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(lowerKeyInSameSegment),
                key
            ) < 0
        );
    }
}
