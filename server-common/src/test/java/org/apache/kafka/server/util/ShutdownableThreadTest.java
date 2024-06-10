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
package org.apache.kafka.server.util;

import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ShutdownableThreadTest {

    @AfterEach
    public void tearDown() {
        Exit.resetExitProcedure();
    }

    @Test
    public void testShutdownWhenCalledAfterThreadStart() throws InterruptedException {
        AtomicReference<Optional<Integer>> statusCodeOption = new AtomicReference<>(Optional.empty());
        Exit.setExitProcedure((statusCode, ignored) -> {
            statusCodeOption.set(Optional.of(statusCode));
            // Sleep until interrupted to emulate the fact that `System.exit()` never returns
            Utils.sleep(Long.MAX_VALUE);
            throw new AssertionError();
        });
        CountDownLatch latch = new CountDownLatch(1);
        ShutdownableThread thread = new ShutdownableThread("shutdownable-thread-test") {
            @Override
            public void doWork() {
                latch.countDown();
                throw new FatalExitError();
            }
        };
        thread.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS), "doWork was not invoked");

        thread.shutdown();
        TestUtils.waitForCondition(() -> statusCodeOption.get().isPresent(), "Status code was not set by exit procedure");
        assertEquals(1, statusCodeOption.get().get());
    }

    @Test
    public void testIsThreadStarted() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ShutdownableThread thread = new ShutdownableThread("shutdownable-thread-test") {
            @Override
            public void doWork() {
                latch.countDown();
            }
        };
        assertFalse(thread.isStarted());
        thread.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS), "doWork was not invoked");
        assertTrue(thread.isStarted());

        thread.shutdown();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testShutdownWhenTestTimesOut(boolean isInterruptible) {
        // Mask the exit procedure so the contents of the "test" can't shut down the JVM
        Exit.setExitProcedure((statusCode, ignored) -> {
            throw new FatalExitError();
        });
        // This latch will be triggered only after the "test" finishes
        CountDownLatch afterTest = new CountDownLatch(1);
        try {
            // This is the "test", which uses a ShutdownableThread with exit procedures masked.
            CountDownLatch startupLatch = new CountDownLatch(1);
            ShutdownableThread thread = new ShutdownableThread("shutdownable-thread-timeout", isInterruptible) {
                @Override
                public void doWork() {
                    // Tell the test that we finished starting, and are ready to shut down.
                    startupLatch.countDown();
                    if (isInterruptible) {
                        // Swallow the interruption sent by the thread
                        try {
                            afterTest.await();
                        } catch (InterruptedException ignored) {
                        }
                    }
                    // Trigger a fatal exit, after the test has completed and the exit procedure masking has been cleaned up
                    try {
                        afterTest.await();
                    } catch (InterruptedException ignored) {
                    }
                    throw new FatalExitError();
                }
            };
            thread.start();
            startupLatch.await();
            // Interrupt ourselves, as if the test was interrupted for timing out
            Thread.currentThread().interrupt();
            thread.shutdown();
            fail("Shutdown should have been interrupted");
        } catch (InterruptedException ignored) {
            // Swallow the interruption, so that the surrounding test passes
        } finally {
            Exit.resetExitProcedure();
        }
        // After the test has stopped waiting for the thread and cleaned up the Exit procedure, let the thread continue
        afterTest.countDown();
    }
}
