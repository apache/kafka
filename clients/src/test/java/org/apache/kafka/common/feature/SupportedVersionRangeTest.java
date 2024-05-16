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

import org.junit.jupiter.api.Test;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the SupportedVersionRange class.
 * Along the way, this suite also includes extensive tests for the base class BaseVersionRange.
 */
public class SupportedVersionRangeTest {
    @Test
    public void testFailDueToInvalidParams() {
        // min and max can't be < 0.
        assertThrows(
            IllegalArgumentException.class,
            () -> new SupportedVersionRange((short) -1, (short) -1));
        // min can't be < 0.
        assertThrows(
            IllegalArgumentException.class,
            () -> new SupportedVersionRange((short) -1, (short) 0));
        // max can't be < 0.
        assertThrows(
            IllegalArgumentException.class,
            () -> new SupportedVersionRange((short) 0, (short) -1));
        // min can't be > max.
        assertThrows(
            IllegalArgumentException.class,
            () -> new SupportedVersionRange((short) 2, (short) 1));
    }

    @Test
    public void testFromToMap() {
        SupportedVersionRange versionRange = new SupportedVersionRange((short) 1, (short) 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());

        Map<String, Short> versionRangeMap = versionRange.toMap();
        assertEquals(
            mkMap(mkEntry("min_version", versionRange.min()), mkEntry("max_version", versionRange.max())),
            versionRangeMap);

        SupportedVersionRange newVersionRange = SupportedVersionRange.fromMap(versionRangeMap);
        assertEquals(1, newVersionRange.min());
        assertEquals(2, newVersionRange.max());
        assertEquals(versionRange, newVersionRange);
    }

    @Test
    public void testFromMapFailure() {
        // min_version can't be < 0.
        Map<String, Short> invalidWithBadMinVersion =
            mkMap(mkEntry("min_version", (short) -1), mkEntry("max_version", (short) 0));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.fromMap(invalidWithBadMinVersion));

        // max_version can't be < 0.
        Map<String, Short> invalidWithBadMaxVersion =
            mkMap(mkEntry("min_version", (short) 0), mkEntry("max_version", (short) -1));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.fromMap(invalidWithBadMaxVersion));

        // min_version and max_version can't be < 0.
        Map<String, Short> invalidWithBadMinMaxVersion =
            mkMap(mkEntry("min_version", (short) -1), mkEntry("max_version", (short) -1));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.fromMap(invalidWithBadMinMaxVersion));

        // min_version can't be > max_version.
        Map<String, Short> invalidWithLowerMaxVersion =
            mkMap(mkEntry("min_version", (short) 2), mkEntry("max_version", (short) 1));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.fromMap(invalidWithLowerMaxVersion));

        // min_version key missing.
        Map<String, Short> invalidWithMinKeyMissing =
            mkMap(mkEntry("max_version", (short) 1));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.fromMap(invalidWithMinKeyMissing));

        // max_version key missing.
        Map<String, Short> invalidWithMaxKeyMissing =
            mkMap(mkEntry("min_version", (short) 1));
        assertThrows(
            IllegalArgumentException.class,
            () -> SupportedVersionRange.fromMap(invalidWithMaxKeyMissing));
    }

    @Test
    public void testToString() {
        assertEquals(
            "SupportedVersionRange[min_version:1, max_version:1]",
            new SupportedVersionRange((short) 1, (short) 1).toString());
        assertEquals(
            "SupportedVersionRange[min_version:1, max_version:2]",
            new SupportedVersionRange((short) 1, (short) 2).toString());
    }

    @Test
    public void testEquals() {
        SupportedVersionRange tested = new SupportedVersionRange((short) 1, (short) 1);
        assertEquals(tested, tested);
        assertNotEquals(tested, new SupportedVersionRange((short) 1, (short) 2));
        assertNotEquals(null, tested);
    }

    @Test
    public void testMinMax() {
        SupportedVersionRange versionRange = new SupportedVersionRange((short) 1, (short) 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());
    }

    @Test
    public void testIsIncompatibleWith() {
        assertFalse(new SupportedVersionRange((short) 1, (short) 1).isIncompatibleWith((short) 1));
        assertFalse(new SupportedVersionRange((short) 1, (short) 4).isIncompatibleWith((short) 2));
        assertFalse(new SupportedVersionRange((short) 1, (short) 4).isIncompatibleWith((short) 1));
        assertFalse(new SupportedVersionRange((short) 1, (short) 4).isIncompatibleWith((short) 4));

        assertTrue(new SupportedVersionRange((short) 2, (short) 3).isIncompatibleWith((short) 1));
        assertTrue(new SupportedVersionRange((short) 2, (short) 3).isIncompatibleWith((short) 4));
    }
}
