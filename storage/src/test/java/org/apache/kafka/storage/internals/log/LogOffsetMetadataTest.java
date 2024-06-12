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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.KafkaException;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.storage.internals.log.LogOffsetMetadata.UNKNOWN_OFFSET_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogOffsetMetadataTest {

    @Test
    void testOnOlderSegment() {
        LogOffsetMetadata metadata1 = new LogOffsetMetadata(1L, 0L, 1);
        LogOffsetMetadata metadata2 = new LogOffsetMetadata(5L, 4L, 2);
        LogOffsetMetadata messageOnlyMetadata = new LogOffsetMetadata(1L);
        assertFalse(UNKNOWN_OFFSET_METADATA.onOlderSegment(UNKNOWN_OFFSET_METADATA));
        assertFalse(metadata1.onOlderSegment(messageOnlyMetadata));
        assertFalse(messageOnlyMetadata.onOlderSegment(metadata1));
        assertFalse(metadata1.onOlderSegment(metadata1));
        assertFalse(metadata2.onOlderSegment(metadata1));
        assertTrue(metadata1.onOlderSegment(metadata2));
    }

    @Test
    void testPositionDiff() {
        LogOffsetMetadata metadata1 = new LogOffsetMetadata(1L);
        LogOffsetMetadata metadata2 = new LogOffsetMetadata(5L, 0L, 5);
        KafkaException exception = assertThrows(KafkaException.class, () -> metadata1.positionDiff(metadata2));
        assertTrue(exception.getMessage().endsWith("since it only has message offset info"));

        exception = assertThrows(KafkaException.class, () -> metadata2.positionDiff(metadata1));
        assertTrue(exception.getMessage().endsWith("since it only has message offset info"));

        LogOffsetMetadata metadata3 = new LogOffsetMetadata(15L, 10L, 5);
        exception = assertThrows(KafkaException.class, () -> metadata3.positionDiff(metadata2));
        assertTrue(exception.getMessage().endsWith("since they are not on the same segment"));

        LogOffsetMetadata metadata4 = new LogOffsetMetadata(40L, 10L, 100);
        assertEquals(95, metadata4.positionDiff(metadata3));
    }

    @Test
    void testMessageOffsetOnly() {
        LogOffsetMetadata metadata1 = new LogOffsetMetadata(1L);
        LogOffsetMetadata metadata2 = new LogOffsetMetadata(1L, 0L, 1);
        assertTrue(UNKNOWN_OFFSET_METADATA.messageOffsetOnly());
        assertFalse(metadata2.messageOffsetOnly());
        assertTrue(metadata1.messageOffsetOnly());
    }

    @Test
    void testOnSameSegment() {
        LogOffsetMetadata metadata1 = new LogOffsetMetadata(1L, 0L, 1);
        LogOffsetMetadata metadata2 = new LogOffsetMetadata(5L, 4L, 2);
        LogOffsetMetadata metadata3 = new LogOffsetMetadata(10L, 4L, 200);
        assertFalse(metadata1.onSameSegment(metadata2));
        assertTrue(metadata2.onSameSegment(metadata3));

        LogOffsetMetadata metadata4 = new LogOffsetMetadata(50);
        LogOffsetMetadata metadata5 = new LogOffsetMetadata(100);
        assertFalse(metadata4.onSameSegment(metadata5));
    }
}