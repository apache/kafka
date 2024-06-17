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
package org.apache.kafka.connect.test.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class ConcurrencyUtils {

    public static final long DEFAULT_LATCH_AWAIT_TIME_MS = TimeUnit.SECONDS.toMillis(5);

    /**
     * {@link CountDownLatch#await(long, TimeUnit) Await} the given latch, failing if the timeout elapses or the wait is interrupted.
     * @param latch the latch to await; may not be null
     * @param timeoutMs the maximum amount of time to wait for the latch, in milliseconds
     * @param message the failure message to use if the timeout elapses or the wait is interrupted; may be null
     */
    public static void awaitLatch(CountDownLatch latch, long timeoutMs, String message) {
        try {
            assertTrue(message, latch.await(timeoutMs, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            throw new AssertionError(message, e);
        }
    }

    /**
     * {@link CountDownLatch#await(long, TimeUnit) Await} the given latch, failing if the
     * {@link #DEFAULT_LATCH_AWAIT_TIME_MS default timeout} elapses or the wait is interrupted.
     * @param latch the latch to await; may not be null
     * @param message the failure message to use if the timeout elapses or the wait is interrupted; may be null
     */
    public static void awaitLatch(CountDownLatch latch, String message) {
        awaitLatch(latch, DEFAULT_LATCH_AWAIT_TIME_MS, message);
    }

}
