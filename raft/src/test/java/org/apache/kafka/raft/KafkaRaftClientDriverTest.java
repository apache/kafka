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
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaRaftClientDriverTest {

    @Test
    public void testShutdown() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaRaftClient<String> raftClient = (KafkaRaftClient<String>) Mockito.mock(KafkaRaftClient.class);
        MockFaultHandler faultHandler = new MockFaultHandler("TestFaultHandler");
        KafkaRaftClientDriver<String> driver = new KafkaRaftClientDriver<>(
            raftClient,
            "test-raft",
            faultHandler,
            new LogContext()
        );

        when(raftClient.isRunning()).thenReturn(true);
        assertTrue(driver.isRunning());

        CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
        when(raftClient.shutdown(5000)).thenReturn(shutdownFuture);

        driver.initiateShutdown();
        assertTrue(driver.isRunning());
        assertTrue(driver.isShutdownInitiated());
        verify(raftClient).shutdown(5000);

        shutdownFuture.complete(null);
        when(raftClient.isRunning()).thenReturn(false);
        driver.run();
        assertFalse(driver.isRunning());
        assertTrue(driver.isShutdownComplete());
        assertNull(faultHandler.firstException());

        driver.shutdown();
        verify(raftClient).close();
    }

    @Test
    public void testUncaughtException() {
        @SuppressWarnings("unchecked")
        KafkaRaftClient<String> raftClient = (KafkaRaftClient<String>) Mockito.mock(KafkaRaftClient.class);
        MockFaultHandler faultHandler = new MockFaultHandler("TestFaultHandler");
        KafkaRaftClientDriver<String> driver = new KafkaRaftClientDriver<>(
            raftClient,
            "test-raft",
            faultHandler,
            new LogContext()
        );

        when(raftClient.isRunning()).thenReturn(true);
        assertTrue(driver.isRunning());

        RuntimeException exception = new RuntimeException();
        Mockito.doThrow(exception).when(raftClient).poll();
        driver.run();

        assertTrue(driver.isShutdownComplete());
        assertTrue(driver.isThreadFailed());
        assertFalse(driver.isRunning());

        Throwable caughtException = faultHandler.firstException().getCause();
        assertEquals(exception, caughtException);
    }

}
