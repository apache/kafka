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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Tests LoggingFaultHandler
 */
public class LoggingFaultHandlerTest {
    /**
     * Test handling faults with and without exceptions.
     */
    @Test
    public void testHandleFault() {
        AtomicInteger counter = new AtomicInteger(0);
        LoggingFaultHandler handler = new LoggingFaultHandler("test", () -> {
            counter.incrementAndGet();
        });
        handler.handleFault("uh oh");
        assertEquals(1, counter.get());
        handler.handleFault("uh oh", new RuntimeException("yikes"));
        assertEquals(2, counter.get());
    }

    /**
     * Test handling an exception in the action callback.
     */
    @Test
    public void testHandleExceptionInAction() {
        LoggingFaultHandler handler = new LoggingFaultHandler("test", () -> {
            throw new RuntimeException("action failed");
        });
        handler.handleFault("uh oh"); // should not throw
        handler.handleFault("uh oh", new RuntimeException("yikes")); // should not throw
    }
}
