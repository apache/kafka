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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShutdownableThreadTest {

    CountDownLatch latch = new CountDownLatch(1);
    ShutdownableThread shutdownableThread = new ShutdownableThread("test") {
        @Override
        public void doWork() {
            latch.countDown();
        }
    };

    @Test
    void testIsStoppedOrShutdownComplete() {
        assertFalse(shutdownableThread.isShutdownComplete());
        assertFalse(shutdownableThread.isStoppedOrShutdownComplete());
    }

    @Test
    void testIsStoppedOrShutdownCompleteWithoutStart() throws InterruptedException {
        shutdownableThread.initiateShutdown();
        shutdownableThread.awaitShutdown();
        assertFalse(shutdownableThread.isShutdownComplete());
        assertTrue(shutdownableThread.isStoppedOrShutdownComplete());
    }

    @Test
    void testIsStoppedOrShutdownCompleteWithStart() throws InterruptedException {
        shutdownableThread.start();
        latch.await();

        shutdownableThread.initiateShutdown();
        shutdownableThread.awaitShutdown();
        assertTrue(shutdownableThread.isShutdownComplete());
        assertTrue(shutdownableThread.isStoppedOrShutdownComplete());
    }
}