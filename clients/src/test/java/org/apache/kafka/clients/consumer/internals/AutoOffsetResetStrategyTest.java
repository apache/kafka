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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AutoOffsetResetStrategyTest {

    @Test
    public void testIsValid() {
        assertTrue(AutoOffsetResetStrategy.isValid("earliest"));
        assertTrue(AutoOffsetResetStrategy.isValid("latest"));
        assertTrue(AutoOffsetResetStrategy.isValid("none"));
        assertFalse(AutoOffsetResetStrategy.isValid("invalid"));
        assertFalse(AutoOffsetResetStrategy.isValid("LATEST"));
        assertFalse(AutoOffsetResetStrategy.isValid(""));
        assertFalse(AutoOffsetResetStrategy.isValid(null));
    }

    @Test
    public void testFromString() {
        assertEquals(AutoOffsetResetStrategy.EARLIEST, AutoOffsetResetStrategy.fromString("earliest"));
        assertEquals(AutoOffsetResetStrategy.LATEST, AutoOffsetResetStrategy.fromString("latest"));
        assertEquals(AutoOffsetResetStrategy.NONE, AutoOffsetResetStrategy.fromString("none"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("invalid"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString("LATEST"));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString(""));
        assertThrows(IllegalArgumentException.class, () -> AutoOffsetResetStrategy.fromString(null));
    }

    @Test
    public void testValidator() {
        AutoOffsetResetStrategy.Validator validator = new AutoOffsetResetStrategy.Validator();
        assertDoesNotThrow(() -> validator.ensureValid("test", "earliest"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "latest"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "none"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "invalid"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "LATEST"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", ""));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", null));
    }

    @Test
    public void testEqualsAndHashCode() {
        AutoOffsetResetStrategy strategy1 = AutoOffsetResetStrategy.fromString("earliest");
        AutoOffsetResetStrategy strategy2 = AutoOffsetResetStrategy.fromString("earliest");
        AutoOffsetResetStrategy strategy3 = AutoOffsetResetStrategy.fromString("latest");

        assertEquals(strategy1, strategy2);
        assertNotEquals(strategy1, strategy3);
        assertEquals(strategy1.hashCode(), strategy2.hashCode());
        assertNotEquals(strategy1.hashCode(), strategy3.hashCode());
    }
}