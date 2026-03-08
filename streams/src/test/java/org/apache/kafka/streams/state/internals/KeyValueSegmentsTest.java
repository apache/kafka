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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KeyValueSegmentsTest extends AbstractRocksDBSegmentsTest<KeyValueSegments> {

    private static final long SEGMENT_INTERVAL = 100L;
    private static final long RETENTION_PERIOD = 4 * SEGMENT_INTERVAL;
    private static final String METRICS_SCOPE = "test-state-id";
    private static final String STORE_NAME = "testStore";

    @Override
    KeyValueSegments getSegments() {
        return new KeyValueSegments(STORE_NAME, METRICS_SCOPE, RETENTION_PERIOD, SEGMENT_INTERVAL);
    }

    @Test
    public void shouldCreateSegmentsOfCorrectType() {
        final KeyValueSegment segment = segments.getOrCreateSegment(0, context);
        assertNotNull(segment);
        assertInstanceOf(KeyValueSegment.class, segment);
        assertEquals(0L, segment.id());
        assertEquals("testStore.0", segment.name());
    }

    @Test
    public void shouldOpenSegmentDB() {
        final KeyValueSegment segment = segments.createSegment(0, segments.segmentName(0));
        assertFalse(segment.isOpen());

        segments.openSegmentDB(segment, context);
        assertTrue(segment.isOpen());

        segment.close();
    }

    @Test
    public void shouldOpenExistingSegments() {
        segments.getOrCreateSegment(0, context);
        segments.getOrCreateSegment(1, context);
        segments.getOrCreateSegment(2, context);
        segments.close();

        final KeyValueSegments newSegments = new KeyValueSegments(STORE_NAME, METRICS_SCOPE, RETENTION_PERIOD, SEGMENT_INTERVAL);
        newSegments.openExisting(context, -1L);

        TestSegments.assertMetricExists("block-cache-capacity", STORE_NAME, METRICS_SCOPE, context);
        TestSegments.assertMetricExists("num-immutable-mem-table", STORE_NAME, METRICS_SCOPE, context);

        final List<KeyValueSegment> allSegments = newSegments.allSegments(true);
        assertEquals(3, allSegments.size());
        assertEquals(0L, allSegments.get(0).id());
        assertEquals(1L, allSegments.get(1).id());
        assertEquals(2L, allSegments.get(2).id());

        final KeyValueSegment segment = newSegments.getOrCreateSegment(3, context);
        assertNotNull(segment);
        assertTrue(segment.isOpen());

        newSegments.close();
    }
}
