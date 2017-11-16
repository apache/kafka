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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class WindowKeySchemaTest {

    private final WindowKeySchema windowKeySchema = new WindowKeySchema();

    @Test
    public void testUpperBoundWithLargeTimestamps() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                WindowStoreUtils.toBinaryKey(
                    new byte[]{0xA},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                WindowStoreUtils.toBinaryKey(
                    new byte[]{0xA, 0xB},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(upper, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xA}, Long.MAX_VALUE, Integer.MAX_VALUE)));
    }

    @Test
    public void testUpperBoundWithKeyBytesLargerThanFirstTimestampByte() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), Long.MAX_VALUE);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                WindowStoreUtils.toBinaryKey(
                    new byte[]{0xA, (byte) 0x8F},
                    Long.MAX_VALUE,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(upper, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}, Long.MAX_VALUE, Integer.MAX_VALUE)));
    }


    @Test
    public void testUpperBoundWithKeyBytesLargerAndSmallerThanFirstTimestampByte() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xC, 0xC, 0x9}), 0x0AffffffffffffffL);

        assertThat(
            "shorter key with max timestamp should be in range",
            upper.compareTo(
                WindowStoreUtils.toBinaryKey(
                    new byte[]{0xC, 0xC},
                    0x0AffffffffffffffL,
                    Integer.MAX_VALUE
                )
            ) >= 0
        );

        assertThat(upper, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xC, 0xC}, 0x0AffffffffffffffL, Integer.MAX_VALUE)));
    }

    @Test
    public void testUpperBoundWithZeroTimestamp() {
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        assertThat(upper, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xA, 0xB, 0xC}, 0, Integer.MAX_VALUE)));
    }

    @Test
    public void testLowerBoundWithZeroTimestamp() {
        Bytes lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        assertThat(lower, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xA, 0xB, 0xC}, 0, 0)));
    }

    @Test
    public void testLowerBoundWithMonZeroTimestamp() {
        Bytes lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 42);
        assertThat(lower, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xA, 0xB, 0xC}, 0, 0)));
    }

    @Test
    public void testLowerBoundMatchesTrailingZeros() {
        Bytes lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), Long.MAX_VALUE - 1);

        assertThat(
            "appending zeros to key should still be in range",
            lower.compareTo(
                WindowStoreUtils.toBinaryKey(
                        new byte[]{0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                        Long.MAX_VALUE - 1,
                        0
                )
            ) < 0
        );

        assertThat(lower, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xA, 0xB, 0xC}, 0, 0)));
    }
}
