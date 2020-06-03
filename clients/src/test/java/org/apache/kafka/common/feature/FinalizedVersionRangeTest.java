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
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the FinalizedVersionRange class.
 *
 * Most of the unit tests required for BaseVersionRange are part of the SupportedVersionRangeTest
 * suite. This suite only tests behavior very specific to FinalizedVersionRange.
 */
public class FinalizedVersionRangeTest {

    @Test
    public void testFromToMap() {
        FinalizedVersionRange versionRange = new FinalizedVersionRange((short) 1, (short) 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());

        Map<String, Short> versionRangeMap = versionRange.toMap();
        assertEquals(
            mkMap(
                mkEntry("min_version_level", versionRange.min()),
                mkEntry("max_version_level", versionRange.max())),
            versionRangeMap);

        FinalizedVersionRange newVersionRange = FinalizedVersionRange.fromMap(versionRangeMap);
        assertEquals(1, newVersionRange.min());
        assertEquals(2, newVersionRange.max());
        assertEquals(versionRange, newVersionRange);
    }

    @Test
    public void testToString() {
        assertEquals("FinalizedVersionRange[min_version_level:1, max_version_level:1]", new FinalizedVersionRange((short) 1, (short) 1).toString());
        assertEquals("FinalizedVersionRange[min_version_level:1, max_version_level:2]", new FinalizedVersionRange((short) 1, (short) 2).toString());
    }

    @Test
    public void testIsCompatibleWith() {
        assertFalse(new FinalizedVersionRange((short) 1, (short) 1).isIncompatibleWith(new SupportedVersionRange((short) 1, (short) 1)));
        assertFalse(new FinalizedVersionRange((short) 2, (short) 3).isIncompatibleWith(new SupportedVersionRange((short) 1, (short) 4)));
        assertFalse(new FinalizedVersionRange((short) 1, (short) 4).isIncompatibleWith(new SupportedVersionRange((short) 1, (short) 4)));

        assertTrue(new FinalizedVersionRange((short) 1, (short) 4).isIncompatibleWith(new SupportedVersionRange((short) 2, (short) 3)));
        assertTrue(new FinalizedVersionRange((short) 1, (short) 4).isIncompatibleWith(new SupportedVersionRange((short) 2, (short) 4)));
        assertTrue(new FinalizedVersionRange((short) 2, (short) 4).isIncompatibleWith(new SupportedVersionRange((short) 2, (short) 3)));
    }

    @Test
    public void testMinMax() {
        FinalizedVersionRange versionRange = new FinalizedVersionRange((short) 1, (short) 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());
    }
}
