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
    public void testSerializeDeserialize() {
        FinalizedVersionRange versionRange = new FinalizedVersionRange(1, 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());

        Map<String, Long> serialized = versionRange.serialize();
        assertEquals(
            mkMap(
                mkEntry("min_version_level", versionRange.min()),
                mkEntry("max_version_level", versionRange.max())),
            serialized
        );

        FinalizedVersionRange deserialized = FinalizedVersionRange.deserialize(serialized);
        assertEquals(1, deserialized.min());
        assertEquals(2, deserialized.max());
        assertEquals(versionRange, deserialized);
    }

    @Test
    public void testToString() {
        assertEquals("FinalizedVersionRange[1, 1]", new FinalizedVersionRange(1, 1).toString());
        assertEquals("FinalizedVersionRange[1, 2]", new FinalizedVersionRange(1, 2).toString());
    }

    @Test
    public void testIsCompatibleWith() {
        assertFalse(new FinalizedVersionRange(1, 1).isIncompatibleWith(new SupportedVersionRange(1, 1)));
        assertFalse(new FinalizedVersionRange(2, 3).isIncompatibleWith(new SupportedVersionRange(1, 4)));
        assertFalse(new FinalizedVersionRange(1, 4).isIncompatibleWith(new SupportedVersionRange(1, 4)));

        assertTrue(new FinalizedVersionRange(1, 4).isIncompatibleWith(new SupportedVersionRange(2, 3)));
        assertTrue(new FinalizedVersionRange(1, 4).isIncompatibleWith(new SupportedVersionRange(2, 4)));
        assertTrue(new FinalizedVersionRange(2, 4).isIncompatibleWith(new SupportedVersionRange(2, 3)));
    }

    @Test
    public void testMinMax() {
        FinalizedVersionRange versionRange = new FinalizedVersionRange(1, 2);
        assertEquals(1, versionRange.min());
        assertEquals(2, versionRange.max());
    }
}
