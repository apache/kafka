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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShareAcknowledgementModeTest {

    @Test
    public void testFromString() {
        assertEquals(ShareAcknowledgementMode.IMPLICIT, ShareAcknowledgementMode.fromString("implicit"));
        assertEquals(ShareAcknowledgementMode.EXPLICIT, ShareAcknowledgementMode.fromString("explicit"));
        assertThrows(IllegalArgumentException.class, () -> ShareAcknowledgementMode.fromString("invalid"));
        assertThrows(IllegalArgumentException.class, () -> ShareAcknowledgementMode.fromString("IMPLICIT"));
        assertThrows(IllegalArgumentException.class, () -> ShareAcknowledgementMode.fromString("EXPLICIT"));
        assertThrows(IllegalArgumentException.class, () -> ShareAcknowledgementMode.fromString(""));
        assertThrows(IllegalArgumentException.class, () -> ShareAcknowledgementMode.fromString(null));
    }

    @Test
    public void testValidator() {
        ShareAcknowledgementMode.Validator validator = new ShareAcknowledgementMode.Validator();
        assertDoesNotThrow(() -> validator.ensureValid("test", "implicit"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "explicit"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "invalid"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "IMPLICIT"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "EXPLICIT"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", ""));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", null));
    }

    @Test
    public void testEqualsAndHashCode() {
        ShareAcknowledgementMode mode1 = ShareAcknowledgementMode.IMPLICIT;
        ShareAcknowledgementMode mode2 = ShareAcknowledgementMode.IMPLICIT;
        ShareAcknowledgementMode mode3 = ShareAcknowledgementMode.EXPLICIT;

        assertEquals(mode1, mode2);
        assertNotEquals(mode1, mode3);
        assertNotEquals(mode2, mode3);

        assertEquals(mode1.hashCode(), mode2.hashCode());
        assertNotEquals(mode1.hashCode(), mode3.hashCode());
    }

}
