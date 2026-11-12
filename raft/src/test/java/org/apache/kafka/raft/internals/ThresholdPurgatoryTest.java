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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.raft.MockExpirationService;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThresholdPurgatoryTest {
    private final MockTime time = new MockTime();
    private final MockExpirationService expirationService = new MockExpirationService(time);
    private final ThresholdPurgatory<Long> purgatory = new ThresholdPurgatory<>(expirationService);

    @Test
    public void testThresholdCompletion() throws Exception {
        var future1 = purgatory.await(3L, 500).toCompletableFuture();
        var future2 = purgatory.await(1L, 500).toCompletableFuture();
        var future3 = purgatory.await(5L, 500).toCompletableFuture();
        assertEquals(3, purgatory.numWaiting());

        long completionTime1 = time.milliseconds();
        purgatory.maybeComplete(1L, completionTime1);
        assertTrue(future2.isDone());
        assertFalse(future1.isDone());
        assertFalse(future3.isDone());
        assertEquals(completionTime1, future2.get());
        assertEquals(2, purgatory.numWaiting());

        time.sleep(100);
        purgatory.maybeComplete(2L, time.milliseconds());
        assertFalse(future1.isDone());
        assertFalse(future3.isDone());

        time.sleep(100);
        long completionTime2 = time.milliseconds();
        purgatory.maybeComplete(3L, completionTime2);
        assertTrue(future1.isDone());
        assertFalse(future3.isDone());
        assertEquals(completionTime2, future1.get());
        assertEquals(1, purgatory.numWaiting());

        time.sleep(100);
        purgatory.maybeComplete(4L, time.milliseconds());
        assertFalse(future3.isDone());

        time.sleep(100);
        long completionTime3 = time.milliseconds();
        purgatory.maybeComplete(5L, completionTime3);
        assertTrue(future3.isDone());
        assertEquals(completionTime3, future3.get());
        assertEquals(0, purgatory.numWaiting());
    }

    @Test
    public void testExpiration() {
        var future1 = purgatory.await(1L, 200).toCompletableFuture();
        var future2 = purgatory.await(1L, 200).toCompletableFuture();
        assertEquals(2, purgatory.numWaiting());

        time.sleep(100);
        var future3 = purgatory.await(5L, 50).toCompletableFuture();
        var future4 = purgatory.await(5L, 200).toCompletableFuture();
        var future5 = purgatory.await(5L, 100).toCompletableFuture();
        assertEquals(5, purgatory.numWaiting());

        time.sleep(50);
        assertFutureThrows(TimeoutException.class, future3);
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());
        assertFalse(future4.isDone());
        assertFalse(future5.isDone());
        assertEquals(4, purgatory.numWaiting());

        time.sleep(50);
        assertFutureThrows(TimeoutException.class, future1);
        assertFutureThrows(TimeoutException.class, future2);
        assertFutureThrows(TimeoutException.class, future5);
        assertFalse(future4.isDone());
        assertEquals(1, purgatory.numWaiting());

        time.sleep(50);
        assertFalse(future4.isDone());
        assertEquals(1, purgatory.numWaiting());

        time.sleep(50);
        assertFutureThrows(TimeoutException.class, future4);
        assertEquals(0, purgatory.numWaiting());
    }

    @Test
    public void testCompleteAll() throws Exception {
        var future1 = purgatory.await(3L, 500).toCompletableFuture();
        var future2 = purgatory.await(1L, 500).toCompletableFuture();
        var future3 = purgatory.await(5L, 500).toCompletableFuture();
        assertEquals(3, purgatory.numWaiting());

        long completionTime = time.milliseconds();
        purgatory.completeAll(completionTime);
        assertEquals(completionTime, future1.get());
        assertEquals(completionTime, future2.get());
        assertEquals(completionTime, future3.get());
        assertEquals(0, purgatory.numWaiting());
    }

    @Test
    public void testCompleteAllExceptionally() {
        var future1 = purgatory.await(3L, 500).toCompletableFuture();
        var future2 = purgatory.await(1L, 500).toCompletableFuture();
        var future3 = purgatory.await(5L, 500).toCompletableFuture();
        assertEquals(3, purgatory.numWaiting());

        purgatory.completeAllExceptionally(new NotLeaderOrFollowerException());
        assertFutureThrows(NotLeaderOrFollowerException.class, future1);
        assertFutureThrows(NotLeaderOrFollowerException.class, future2);
        assertFutureThrows(NotLeaderOrFollowerException.class, future3);
        assertEquals(0, purgatory.numWaiting());
    }

    @Test
    public void testMultipleAwaitersWithSameThreshold() throws Exception {
        var future1 = purgatory.await(3L, 500).toCompletableFuture();
        var future2 = purgatory.await(3L, 500).toCompletableFuture();
        var future3 = purgatory.await(3L, 500).toCompletableFuture();
        assertEquals(3, purgatory.numWaiting());

        long completionTime = time.milliseconds();
        purgatory.maybeComplete(3L, completionTime);
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
        assertTrue(future3.isDone());
        assertEquals(completionTime, future1.get());
        assertEquals(completionTime, future2.get());
        assertEquals(completionTime, future3.get());
        assertEquals(0, purgatory.numWaiting());
    }

    @Test
    public void testMaybeCompleteWithHigherValue() throws Exception {
        var future1 = purgatory.await(1L, 500).toCompletableFuture();
        var future2 = purgatory.await(3L, 500).toCompletableFuture();
        var future3 = purgatory.await(5L, 500).toCompletableFuture();
        assertEquals(3, purgatory.numWaiting());

        long completionTime = time.milliseconds();
        purgatory.maybeComplete(10L, completionTime);
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
        assertTrue(future3.isDone());
        assertEquals(completionTime, future1.get());
        assertEquals(completionTime, future2.get());
        assertEquals(completionTime, future3.get());
        assertEquals(0, purgatory.numWaiting());
    }

    @Test
    public void testNumWaitingWithMixedCompletions() {
        purgatory.await(1L, 500);
        purgatory.await(2L, 500);
        purgatory.await(3L, 500);
        purgatory.await(4L, 500);
        assertEquals(4, purgatory.numWaiting());

        purgatory.maybeComplete(1L, time.milliseconds());
        assertEquals(3, purgatory.numWaiting());

        purgatory.maybeComplete(3L, time.milliseconds());
        assertEquals(1, purgatory.numWaiting());

        purgatory.completeAll(time.milliseconds());
        assertEquals(0, purgatory.numWaiting());
    }

    @Test
    public void testExpirationReducesWaitingCount() {
        var future1 = purgatory.await(1L, 100).toCompletableFuture();
        var future2 = purgatory.await(2L, 100).toCompletableFuture();
        assertEquals(2, purgatory.numWaiting());

        time.sleep(100);
        assertFutureThrows(TimeoutException.class, future1);
        assertFutureThrows(TimeoutException.class, future2);
        assertEquals(0, purgatory.numWaiting());
    }

}
