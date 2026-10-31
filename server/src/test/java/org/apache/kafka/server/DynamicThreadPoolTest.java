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
package org.apache.kafka.server;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.config.ServerConfigs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamicThreadPoolTest {

    private static final String CONFIG = ServerConfigs.NUM_IO_THREADS_CONFIG;

    @Test
    public void testValidateThreadCountWithBounds() {
        assertDoesNotThrow(() -> DynamicThreadPool.validateThreadCount(CONFIG, 8, 8, true));
        assertDoesNotThrow(() -> DynamicThreadPool.validateThreadCount(CONFIG, 8, 4, true));
        assertDoesNotThrow(() -> DynamicThreadPool.validateThreadCount(CONFIG, 8, 16, true));

        assertThrowsWithMessage(8, 3, true, "value should be at least half the current value 8");
        assertThrowsWithMessage(8, 17, true, "value should not be greater than double the current value 8");
        assertThrowsWithMessage(1, 0, true, "value should be at least 1");
    }

    @Test
    public void testValidateThreadCountWithoutBounds() {
        assertDoesNotThrow(() -> DynamicThreadPool.validateThreadCount(CONFIG, 16, 6, false));
        assertDoesNotThrow(() -> DynamicThreadPool.validateThreadCount(CONFIG, 2, 16, false));

        assertThrowsWithMessage(16, 0, false, "value should be at least 1");
    }

    private static void assertThrowsWithMessage(int currentValue, int newValue, boolean enforceBounds, String message) {
        ConfigException e = assertThrows(ConfigException.class,
            () -> DynamicThreadPool.validateThreadCount(CONFIG, currentValue, newValue, enforceBounds));
        assertTrue(e.getMessage().contains("Dynamic thread count update validation failed for " + CONFIG + "=" + newValue), e.getMessage());
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }
}
