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
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShareAcquireModeTest {

    @Test
    public void testFromString() {
        assertEquals(ShareAcquireMode.BATCH_OPTIMIZED, ShareAcquireMode.of("batch_optimized"));
        assertEquals(ShareAcquireMode.BATCH_OPTIMIZED, ShareAcquireMode.of("BATCH_OPTIMIZED"));
        assertEquals(ShareAcquireMode.RECORD_LIMIT, ShareAcquireMode.of("record_limit"));
        assertEquals(ShareAcquireMode.RECORD_LIMIT, ShareAcquireMode.of("RECORD_LIMIT"));
        assertThrows(IllegalArgumentException.class, () -> ShareAcquireMode.of("invalid_mode"));
        assertThrows(IllegalArgumentException.class, () -> ShareAcquireMode.of(""));
        assertThrows(IllegalArgumentException.class, () -> ShareAcquireMode.of(null));
    }

    @Test
    public void testValidator() {
        ShareAcquireMode.Validator validator = new ShareAcquireMode.Validator();
        assertDoesNotThrow(() -> validator.ensureValid("test", "batch_optimized"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "BATCH_OPTIMIZED"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "record_limit"));
        assertDoesNotThrow(() -> validator.ensureValid("test", "RECORD_LIMIT"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", "invalid_mode"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", ""));
        assertThrows(ConfigException.class, () -> validator.ensureValid("test", null));
    }

    @Test
    public void testValidatorToString() {
        ShareAcquireMode.Validator validator = new ShareAcquireMode.Validator();
        assertEquals("[batch_optimized, record_limit]", validator.toString());
    }
}
