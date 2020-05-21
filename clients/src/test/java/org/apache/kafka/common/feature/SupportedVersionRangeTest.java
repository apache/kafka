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

package org.apache.kafka.common.feature;

import java.util.Map;

import org.junit.Test;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the SupportedVersionRange class.
 * Along the way, this suite also includes extensive tests for the base class BaseVersionRange.
 */
public class SupportedVersionRangeTest {
    @Test
    public void testFailDueToInvalidParams() {
        // min and max can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new SupportedVersionRange(0, 0));
        // min can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new SupportedVersionRange(0, 1));
        // max can't be < 1.
        assertThrows(
            IllegalArgumentException.class,
            () -> new SupportedVersionRange(1, 0));
        // min can't be > max.
        assertThrows(
            IllegalArgumentException.class,
            () -> new SupportedVersionRange(2, 1));
    }

    @Test
    public void testSerializeDeserializeTest() {
        SupportedVersionRange versionRange = new SupportedVersionRange(1, 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());

        Map<String, Long> serialized = versionRange.serialize();
        assertEquals(
            mkMap(mkEntry("min_version", versionRange.min()), mkEntry("max_version", versionRange.max())),
            serialized);

        SupportedVersionRange deserialized = SupportedVersionRange.deserialize(serialized);
        assertEquals(1, deserialized.min());
        assertEquals(2, deserialized.max());
        assertEquals(versionRange, deserialized);
    }

    @Test
    public void testDeserializationFailureTest() {
        // min_version can't be < 1.
        Map<String, Long> invalidWithBadMinVersion = mkMap(mkEntry("min_version", 0L), mkEntry("max_version", 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.deserialize(invalidWithBadMinVersion));

        // max_version can't be < 1.
        Map<String, Long> invalidWithBadMaxVersion = mkMap(mkEntry("min_version", 1L), mkEntry("max_version", 0L));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.deserialize(invalidWithBadMaxVersion));

        // min_version and max_version can't be < 1.
        Map<String, Long> invalidWithBadMinMaxVersion = mkMap(mkEntry("min_version", 0L), mkEntry("max_version", 0L));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.deserialize(invalidWithBadMinMaxVersion));

        // min_version can't be > max_version.
        Map<String, Long> invalidWithLowerMaxVersion = mkMap(mkEntry("min_version", 2L), mkEntry("max_version", 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.deserialize(invalidWithLowerMaxVersion));

        // min_version key missing.
        Map<String, Long> invalidWithMinKeyMissing = mkMap(mkEntry("max_version", 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.deserialize(invalidWithMinKeyMissing));

        // max_version key missing.
        Map<String, Long> invalidWithMaxKeyMissing = mkMap(mkEntry("min_version", 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.deserialize(invalidWithMaxKeyMissing));
    }

    @Test
    public void testToString() {
        assertEquals("SupportedVersionRange[1, 1]", new SupportedVersionRange(1, 1).toString());
        assertEquals("SupportedVersionRange[1, 2]", new SupportedVersionRange(1, 2).toString());
    }

    @Test
    public void testEquals() {
        assertTrue(new SupportedVersionRange(1, 1).equals(new SupportedVersionRange(1, 1)));
        assertFalse(new SupportedVersionRange(1, 1).equals(new SupportedVersionRange(1, 2)));
        assertFalse(new SupportedVersionRange(1, 1).equals(null));
    }

    @Test
    public void testMinMax() {
        SupportedVersionRange versionRange = new SupportedVersionRange(1, 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());
    }
}
