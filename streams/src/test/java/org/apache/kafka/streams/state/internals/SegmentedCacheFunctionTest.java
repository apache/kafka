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

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

// TODO: this test coverage does not consider session serde yet
public class SegmentedCacheFunctionTest {

    private static final int SEGMENT_INTERVAL = 17;
    private static final int TIMESTAMP = 736213517;

    private static final Bytes THE_KEY = WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, TIMESTAMP, 42);
    private final static Bytes THE_CACHE_KEY = Bytes.wrap(
        ByteBuffer.allocate(8 + THE_KEY.get().length)
            .putLong(TIMESTAMP / SEGMENT_INTERVAL)
            .put(THE_KEY.get()).array()
    );

    private final SegmentedCacheFunction cacheFunction = new SegmentedCacheFunction(new WindowKeySchema(), SEGMENT_INTERVAL);

    @Test
    public void key() {
        assertThat(
            cacheFunction.key(THE_CACHE_KEY),
            equalTo(THE_KEY)
        );
    }

    @Test
    public void cacheKey() {
        final long segmentId = TIMESTAMP / SEGMENT_INTERVAL;

        final Bytes actualCacheKey = cacheFunction.cacheKey(THE_KEY);
        final ByteBuffer buffer = ByteBuffer.wrap(actualCacheKey.get());

        assertThat(buffer.getLong(), equalTo(segmentId));

        final byte[] actualKey = new byte[buffer.remaining()];
        buffer.get(actualKey);
        assertThat(Bytes.wrap(actualKey), equalTo(THE_KEY));
    }

    @Test
    public void testRoundTripping() {
        assertThat(
            cacheFunction.key(cacheFunction.cacheKey(THE_KEY)),
            equalTo(THE_KEY)
        );

        assertThat(
            cacheFunction.cacheKey(cacheFunction.key(THE_CACHE_KEY)),
            equalTo(THE_CACHE_KEY)
        );
    }

    @Test
    public void compareSegmentedKeys() {
        assertThat(
            "same key in same segment should be ranked the same",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(THE_KEY),
                THE_KEY
            ) == 0
        );

        final Bytes sameKeyInPriorSegment = WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xC}, 1234, 42);

        assertThat(
            "same keys in different segments should be ordered according to segment",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(sameKeyInPriorSegment),
                THE_KEY
            ) < 0
        );

        assertThat(
            "same keys in different segments should be ordered according to segment",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(THE_KEY),
                sameKeyInPriorSegment
            ) > 0
        );

        final Bytes lowerKeyInSameSegment = WindowKeySchema.toStoreKeyBinary(new byte[]{0xA, 0xB, 0xB}, TIMESTAMP - 1, 0);

        assertThat(
            "different keys in same segments should be ordered according to key",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(THE_KEY),
                lowerKeyInSameSegment
            ) > 0
        );

        assertThat(
            "different keys in same segments should be ordered according to key",
            cacheFunction.compareSegmentedKeys(
                cacheFunction.cacheKey(lowerKeyInSameSegment),
                THE_KEY
            ) < 0
        );
    }

}
