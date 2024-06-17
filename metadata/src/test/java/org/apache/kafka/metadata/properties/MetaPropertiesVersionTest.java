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

package org.apache.kafka.metadata.properties;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final public class MetaPropertiesVersionTest {
    @Test
    public void testV0ToNumber() {
        assertEquals(0, MetaPropertiesVersion.V0.number());
    }

    @Test
    public void testV0ToNumberString() {
        assertEquals("0", MetaPropertiesVersion.V0.numberString());
    }

    @Test
    public void testV0FromNumber() {
        assertEquals(MetaPropertiesVersion.V0, MetaPropertiesVersion.fromNumber(0));
    }

    @Test
    public void testV0FromNumberString() {
        assertEquals(MetaPropertiesVersion.V0, MetaPropertiesVersion.fromNumberString("0"));
    }

    @Test
    public void testV1ToNumber() {
        assertEquals(1, MetaPropertiesVersion.V1.number());
    }

    @Test
    public void testV1ToNumberString() {
        assertEquals("1", MetaPropertiesVersion.V1.numberString());
    }

    @Test
    public void testV1FromNumber() {
        assertEquals(MetaPropertiesVersion.V1, MetaPropertiesVersion.fromNumber(1));
    }

    @Test
    public void testV1FromNumberString() {
        assertEquals(MetaPropertiesVersion.V1, MetaPropertiesVersion.fromNumberString("1"));
    }

    @Test
    public void testFromInvalidNumber() {
        assertEquals("Unknown meta.properties version number 2",
            assertThrows(RuntimeException.class,
                () -> MetaPropertiesVersion.fromNumber(2)).getMessage());
    }

    @Test
    public void testFromInvalidString() {
        assertEquals("Invalid meta.properties version string 'orange'",
            assertThrows(RuntimeException.class,
                () -> MetaPropertiesVersion.fromNumberString("orange")).getMessage());
    }

    @Test
    public void testHasBrokerId() {
        assertTrue(MetaPropertiesVersion.V0.hasBrokerId());
        assertFalse(MetaPropertiesVersion.V1.hasBrokerId());
    }

    @Test
    public void testAlwaysHasNodeId() {
        assertFalse(MetaPropertiesVersion.V0.alwaysHasNodeId());
        assertTrue(MetaPropertiesVersion.V1.alwaysHasNodeId());
    }

    @Test
    public void testAlwaysHasClusterId() {
        assertFalse(MetaPropertiesVersion.V0.alwaysHasClusterId());
        assertTrue(MetaPropertiesVersion.V1.alwaysHasClusterId());
    }
}
