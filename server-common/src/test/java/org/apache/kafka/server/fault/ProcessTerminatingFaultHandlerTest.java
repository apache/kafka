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

package org.apache.kafka.server.fault;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Exit.Procedure;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final public class ProcessTerminatingFaultHandlerTest {
    private static Procedure terminatingProcedure(AtomicBoolean called) {
        return (statusCode, message) -> {
            assertEquals(1, statusCode);
            assertNull(message);
            called.set(true);
        };
    }

    @Test
    public void testExitIsCalled() {
        AtomicBoolean exitCalled = new AtomicBoolean(false);
        Exit.setExitProcedure(terminatingProcedure(exitCalled));

        AtomicBoolean actionCalled = new AtomicBoolean(false);
        Runnable action = () -> {
            assertFalse(exitCalled.get());
            actionCalled.set(true);
        };

        try {
            new ProcessTerminatingFaultHandler.Builder()
                .setShouldHalt(false)
                .setAction(action)
                .build()
                .handleFault("", null);
        } finally {
            Exit.resetExitProcedure();
        }

        assertTrue(exitCalled.get());
        assertTrue(actionCalled.get());
    }

    @Test
    public void testHaltIsCalled() {
        AtomicBoolean haltCalled = new AtomicBoolean(false);
        Exit.setHaltProcedure(terminatingProcedure(haltCalled));

        AtomicBoolean actionCalled = new AtomicBoolean(false);
        Runnable action = () -> {
            assertFalse(haltCalled.get());
            actionCalled.set(true);
        };

        try {
            new ProcessTerminatingFaultHandler.Builder()
                .setAction(action)
                .build()
                .handleFault("", null);
        } finally {
            Exit.resetHaltProcedure();
        }

        assertTrue(haltCalled.get());
        assertTrue(actionCalled.get());
    }
}
