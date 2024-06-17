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

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RocksDbVersionedKeyValueBytesStoreSupplierTest {

    private final static String STORE_NAME = "versioned_store";

    @Test
    public void shouldUseDefaultSegmentInterval() {
        verifyExpectedSegmentInterval(0L, 2_000L);
        verifyExpectedSegmentInterval(1_000L, 2_000L);
        verifyExpectedSegmentInterval(6_000L, 2_000L);
        verifyExpectedSegmentInterval(30_000L, 10_000L);
        verifyExpectedSegmentInterval(60_000L, 20_000L);
        verifyExpectedSegmentInterval(80_000L, 20_000L);
        verifyExpectedSegmentInterval(100_000L, 20_000L);
        verifyExpectedSegmentInterval(200_000L, 40_000L);
        verifyExpectedSegmentInterval(300_000L, 60_000L);
        verifyExpectedSegmentInterval(600_000L, 60_000L);
        verifyExpectedSegmentInterval(720_000L, 60_000L);
        verifyExpectedSegmentInterval(1200_000L, 100_000L);
        verifyExpectedSegmentInterval(3600_000L, 300_000L);
        verifyExpectedSegmentInterval(6000_000L, 300_000L);
        verifyExpectedSegmentInterval(7200_000L, 300_000L);
        verifyExpectedSegmentInterval(24 * 3600_000L, 3600_000L);
    }

    private void verifyExpectedSegmentInterval(final long historyRetention, final long expectedSegmentInterval) {
        assertThat(
            new RocksDbVersionedKeyValueBytesStoreSupplier(STORE_NAME, historyRetention).segmentIntervalMs(),
            is(expectedSegmentInterval));
    }
}