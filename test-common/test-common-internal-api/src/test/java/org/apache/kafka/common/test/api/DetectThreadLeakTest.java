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

package org.apache.kafka.common.test.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DetectThreadLeakTest {

    private static class LeakThread implements Runnable {
        @Override
        public void run() {
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                // this can be neglected
            }
        }
    }

    @Test
    public void testThreadLeak() throws InterruptedException {
        DetectThreadLeak detectThreadLeak = DetectThreadLeak.of(thread -> true);
        Thread leakThread = new Thread(new LeakThread());
        try {
            leakThread.start();
            assertTrue(detectThreadLeak.newThreads().contains(leakThread));
            leakThread.interrupt();
        } finally {
            leakThread.join();
        }
        assertFalse(leakThread.isAlive(), "Can't interrupt the thread");
        assertFalse(detectThreadLeak.newThreads().contains(leakThread));
    }

    @Test
    public void testDetectThreadLeakWithOverrideExpectedThreadNames() throws InterruptedException {
        String threadName = "test-thread";
        DetectThreadLeak detectThreadLeak = DetectThreadLeak.of(thread -> !thread.getName().equals(threadName));
        Thread leakThread = new Thread(new LeakThread(), threadName);
        try {
            leakThread.start();
            assertFalse(detectThreadLeak.newThreads().contains(leakThread));
            leakThread.interrupt();
        } finally {
            leakThread.join();
        }
        assertFalse(leakThread.isAlive(), "Can't interrupt the thread");
    }
}
