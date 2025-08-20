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

import org.apache.kafka.common.errors.FencedInstanceIdException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseHeartbeatThreadTest {

    @Test
    public void testIsEnabled() {
        try (BaseHeartbeatThread baseHeartbeatThread = new BaseHeartbeatThread("test", true)) {
            assertFalse(baseHeartbeatThread.isEnabled());

            baseHeartbeatThread.enable();
            assertTrue(baseHeartbeatThread.isEnabled());

            baseHeartbeatThread.disable();
            assertFalse(baseHeartbeatThread.isEnabled());
        }
    }

    @Test
    public void testIsFailed() {
        try (BaseHeartbeatThread baseHeartbeatThread = new BaseHeartbeatThread("test", true)) {
            assertFalse(baseHeartbeatThread.isFailed());
            assertNull(baseHeartbeatThread.failureCause());

            FencedInstanceIdException exception = new FencedInstanceIdException("test");
            baseHeartbeatThread.setFailureCause(exception);
            assertTrue(baseHeartbeatThread.isFailed());
            assertEquals(exception, baseHeartbeatThread.failureCause());
        }
    }

    @Test
    public void testIsClosed() {
        try (BaseHeartbeatThread baseHeartbeatThread = new BaseHeartbeatThread("test", true)) {
            assertFalse(baseHeartbeatThread.isClosed());

            baseHeartbeatThread.close();
            assertTrue(baseHeartbeatThread.isClosed());
        }
    }
}
