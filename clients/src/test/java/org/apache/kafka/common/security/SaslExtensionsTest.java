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
package org.apache.kafka.common.security;

import java.util.Collections;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SaslExtensionsTest {
    Map<String, String> map;

    @BeforeEach
    public void setUp() {
        this.map = new HashMap<>();
        this.map.put("what", "42");
        this.map.put("who", "me");
    }

    @Test
    public void testReturnedMapIsImmutable() {
        SaslExtensions extensions = new SaslExtensions(this.map);
        assertThrows(UnsupportedOperationException.class, () -> extensions.map().put("hello", "test"));
    }

    @Test
    public void testCannotAddValueToMapReferenceAndGetFromExtensions() {
        SaslExtensions extensions = new SaslExtensions(this.map);

        assertNull(extensions.map().get("hello"));
        this.map.put("hello", "42");
        assertNull(extensions.map().get("hello"));
    }

    /**
     * Tests that even when using the same underlying values in the map, two {@link SaslExtensions}
     * are considered unique.
     *
     * @see SaslExtensions class-level documentation
     */
    @Test
    public void testExtensionsWithEqualValuesAreUnique() {
        // If the maps are distinct objects but have the same underlying values, the SaslExtension
        // objects should still be unique.
        assertNotEquals(new SaslExtensions(Collections.singletonMap("key", "value")),
            new SaslExtensions(Collections.singletonMap("key", "value")),
            "SaslExtensions with unique maps should be unique");

        // If the maps are the same object (with the same underlying values), the SaslExtension
        // objects should still be unique.
        assertNotEquals(new SaslExtensions(map),
            new SaslExtensions(map),
            "SaslExtensions with duplicate maps should be unique");

        // If the maps are empty, the SaslExtension objects should still be unique.
        assertNotEquals(SaslExtensions.empty(),
            SaslExtensions.empty(),
            "SaslExtensions returned from SaslExtensions.empty() should be unique");
    }
}
