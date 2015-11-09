/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.util;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShutdownableThreadTest {

    @Test
    public void testGracefulShutdown() throws InterruptedException {
        ShutdownableThread thread = new ShutdownableThread("graceful") {
            @Override
            public void execute() {
                while (getRunning()) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        // Ignore
                    }
                }
            }
        };
        thread.start();
        Thread.sleep(10);
        assertTrue(thread.gracefulShutdown(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testForcibleShutdown() throws InterruptedException {
        final CountDownLatch startedLatch = new CountDownLatch(1);
        ShutdownableThread thread = new ShutdownableThread("forcible") {
            @Override
            public void execute() {
                try {
                    startedLatch.countDown();
                    Thread.sleep(100000);
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
        };
        thread.start();
        startedLatch.await();
        thread.forceShutdown();
        // Not all threads can be forcibly stopped since interrupt() doesn't work on threads in
        // certain conditions, but in this case we know the thread is interruptible so we should be
        // able join() it
        thread.join(1000);
        assertFalse(thread.isAlive());
    }
}
