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

package org.apache.kafka.metadata;

import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(value = 40)
public class FeatureManagerTest {
    private static final Map<String, VersionRange> SUPPORTED = new HashMap<>();

    static {
        SUPPORTED.put("foo", new VersionRange((short) 1, (short) 2));
        SUPPORTED.put("bar", new VersionRange((short) 2, (short) 10));
    }

    @Test
    public void testValidateFinalizedFeatures() {
        FeatureManager manager = new FeatureManager(SUPPORTED, Collections.emptyMap());
        manager.validateFinalizedFeatures(Collections.emptyMap());
        manager.validateFinalizedFeatures(Collections.
            singletonMap("foo", new VersionRange((short) 2, (short) 2)));
        manager.validateFinalizedFeatures(SUPPORTED);
        assertEquals("Unable to finalize quux because that feature is not supported " +
            "by this node.", assertThrows(InvalidUpdateVersionException.class,
                () -> manager.validateFinalizedFeatures(Collections.
                singletonMap("quux", new VersionRange((short) 2, (short) 2)))).getMessage());
        assertEquals("Unable to finalize foo because the requested range 1-3 " +
                "is outside the supported range 1-2.",
            assertThrows(InvalidUpdateVersionException.class,
                () -> manager.validateFinalizedFeatures(Collections.
                singletonMap("foo", new VersionRange((short) 1, (short) 3)))).getMessage());
    }

    @Test
    public void testEquals() {
        FeatureManager manager = new FeatureManager(SUPPORTED, Collections.emptyMap());
        assertEquals(manager, manager);
        FeatureManager manager2 = new FeatureManager(SUPPORTED, Collections.emptyMap());
        assertEquals(manager, manager2);
        FeatureManager manager3 = new FeatureManager(SUPPORTED, Collections.
            singletonMap("foo", new VersionRange((short) 2, (short) 2)));
        assertNotEquals(manager3, manager2);
    }

    @Test
    public void testToString() {
        FeatureManager manager = new FeatureManager(SUPPORTED, Collections.
            singletonMap("foo", new VersionRange((short) 2, (short) 2)));
        assertEquals("FeatureManager(supportedFeatures={bar: 2-10, foo: 1-2}, " +
            "finalizedFeatures={foo: 2})", manager.toString());
    }
}
