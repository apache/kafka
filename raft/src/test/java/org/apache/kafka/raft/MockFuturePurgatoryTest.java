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

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MockFuturePurgatoryTest {
    private final MockTime time = new MockTime();
    private final MockFuturePurgatory<Long> purgatory = new MockFuturePurgatory<>(time);

    @Test
    public void testCompletion() throws Exception {
        CompletableFuture<Long> future1 = purgatory.await(val -> val > 1L, 500L);
        assertEquals(1, purgatory.numWaiting());

        CompletableFuture<Long> future2 = purgatory.await(val -> val > 2L, 1000L);
        assertEquals(2, purgatory.numWaiting());

        purgatory.maybeComplete(3L, 1L);
        assertEquals(0, purgatory.numWaiting());
        assertTrue(future1.isDone());
        assertEquals(1L, future1.get().longValue());
        assertTrue(future2.isDone());
        assertEquals(1L, future2.get().longValue());
    }

    @Test
    public void testCompletionExceptionally() {
        CompletableFuture<Long> future1 = purgatory.await(val -> val > 1L, 500L);
        assertEquals(1, purgatory.numWaiting());

        CompletableFuture<Long> future2 = purgatory.await(val -> val > 2L, 1000L);
        assertEquals(2, purgatory.numWaiting());

        Throwable exception = new Throwable();

        purgatory.completeAllExceptionally(exception);
        assertEquals(0, purgatory.numWaiting());
        assertTrue(future1.isDone());
        ExecutionException thrown1 = assertThrows(ExecutionException.class, future1::get);
        assertEquals(exception, thrown1.getCause());

        assertEquals(0, purgatory.numWaiting());
        assertTrue(future2.isDone());
        ExecutionException thrown2 = assertThrows(ExecutionException.class, future2::get);
        assertEquals(exception, thrown2.getCause());
    }

    @Test
    public void testExpiration() {
        CompletableFuture<Long> future1 = purgatory.await(val -> val > 1L, 500L);
        CompletableFuture<Long> future2 = purgatory.await(val -> val > 2L, 500L);
        CompletableFuture<Long> future3 = purgatory.await(val -> val > 3L, 1000L);
        assertEquals(3, purgatory.numWaiting());

        time.sleep(500);
        assertTrue(future1.isDone());
        assertFutureThrows(future1, TimeoutException.class);

        assertTrue(future2.isDone());
        assertFutureThrows(future2, TimeoutException.class);

        assertEquals(1, purgatory.numWaiting());

        time.sleep(500);
        assertTrue(future3.isDone());
        assertFutureThrows(future3, TimeoutException.class);
        assertEquals(0, purgatory.numWaiting());
    }

}
