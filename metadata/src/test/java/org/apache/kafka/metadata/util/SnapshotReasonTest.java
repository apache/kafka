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
package org.apache.kafka.metadata.util;

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class SnapshotReasonTest {
    @Test
    public void testUnknown() {
        assertEquals("unknown reason", SnapshotReason.UNKNOWN.toString());
    }

    @Test
    public void testMaxBytesExceeded() {
        long bytes = 1000;
        long maxBytes = 900;
        String expectedMessage = "1000 bytes exceeded the maximum bytes of 900";

        assertEquals(expectedMessage, SnapshotReason.maxBytesExceeded(bytes, maxBytes).toString());
    }

    @Test
    public void testMaxIntervalExceeded() {
        long interval = 1000;
        long maxInterval = 900;
        String expectedMessage = "1000 ms exceeded the maximum snapshot interval of 900 ms";

        assertEquals(expectedMessage, SnapshotReason.maxIntervalExceeded(interval, maxInterval).toString());
    }

    @Test
    public void testMetadataVersionChanged() {
        MetadataVersion metadataVersion = MetadataVersion.IBP_3_3_IV3;
        String expectedMessage = "metadata version was changed to 3.3-IV3";

        assertEquals(expectedMessage, SnapshotReason.metadataVersionChanged(metadataVersion).toString());
    }

    @Test
    public void testJoinReasons() {
        long bytes = 1000;
        long maxBytes = 900;
        long interval = 1000;
        long maxInterval = 900;
        MetadataVersion metadataVersion = MetadataVersion.IBP_3_3_IV3;

        List<SnapshotReason> reasons = Arrays.asList(
            SnapshotReason.UNKNOWN,
            SnapshotReason.maxBytesExceeded(bytes, maxBytes),
            SnapshotReason.maxIntervalExceeded(interval, maxInterval),
            SnapshotReason.metadataVersionChanged(metadataVersion)
        );

        String joinedReasons = SnapshotReason.stringFromReasons(reasons);

        assertTrue(joinedReasons.contains("unknown reason"), joinedReasons);
        assertTrue(joinedReasons.contains("1000 bytes exceeded the maximum bytes of 900"), joinedReasons);
        assertTrue(joinedReasons.contains("1000 ms exceeded the maximum snapshot interval of 900 ms"), joinedReasons);
        assertTrue(joinedReasons.contains("metadata version was changed to 3.3-IV3"), joinedReasons);
    }
}
