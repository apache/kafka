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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.server.util.MockEventExecutor;

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
        MockEventExecutor eventExecutor = new MockEventExecutor(new MockTime());
        KafkaRaftClientDriver<String> driver = new KafkaRaftClientDriver<>(
            raftClient,
            eventExecutor,
            faultHandler,
            new LogContext()
        );

        when(raftClient.isRunning()).thenReturn(true);
        assertTrue(driver.isRunning());

        // Start the driver — this submits the first poll event
        driver.start();

        // Execute the poll event — client.poll() is called, then re-submits since isRunning=true
        assertTrue(eventExecutor.poll());
        verify(raftClient).poll();

        // Set up shutdown
        CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
        when(raftClient.shutdown(5000)).thenReturn(shutdownFuture);
        shutdownFuture.complete(null);
        when(raftClient.isRunning()).thenReturn(false);

        // Drain the re-submitted poll event — client.poll() is called again,
        // but isRunning=false so doPoll won't re-submit. This is needed because
        // MockEventExecutor requires manual polling to drain pending tasks (unlike
        // DefaultEventExecutor which has a background thread).
        assertTrue(eventExecutor.poll());

        // Shutdown the driver — this calls client.shutdown() and eventExecutor.shutdown()
        driver.shutdown();

        assertFalse(driver.isRunning());
        verify(raftClient, Mockito.times(2)).poll();
        verify(raftClient).shutdown(5000);
        verify(raftClient).close();
        assertNull(faultHandler.firstException());
    }

    @Test
    public void testUncaughtException() {
        @SuppressWarnings("unchecked")
        KafkaRaftClient<String> raftClient = (KafkaRaftClient<String>) Mockito.mock(KafkaRaftClient.class);
        MockFaultHandler faultHandler = new MockFaultHandler("TestFaultHandler");
        MockEventExecutor eventExecutor = new MockEventExecutor(new MockTime());
        KafkaRaftClientDriver<String> driver = new KafkaRaftClientDriver<>(
            raftClient,
            eventExecutor,
            faultHandler,
            new LogContext()
        );

        when(raftClient.isRunning()).thenReturn(true);
        assertTrue(driver.isRunning());

        // Make client.poll() throw an exception
        RuntimeException exception = new RuntimeException();
        Mockito.doThrow(exception).when(raftClient).poll();

        // Start the driver and execute the poll event
        driver.start();
        assertTrue(eventExecutor.poll());

        // The fault handler should have recorded the exception
        Throwable caughtException = faultHandler.firstException().getCause();
        assertEquals(exception, caughtException);

        // The poll event should not have been re-submitted (doPoll returns after fault)
        assertFalse(eventExecutor.poll());
    }
}
