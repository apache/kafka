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

package org.apache.kafka.soak.common;

import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SoakUtilTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testAwaitTerminationUninterruptibly() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final ScheduledExecutorService executorService =
            Executors.newSingleThreadScheduledExecutor();
        executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                countDownLatch.await();
                return null;
            }
        });
        executorService.shutdown();
        final AtomicBoolean threadFinished = new AtomicBoolean(false);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                SoakUtil.awaitTerminationUninterruptibly(executorService);
                threadFinished.set(true);
            }
        });
        thread.start();
        assertFalse(threadFinished.get());
        thread.interrupt();
        while (!thread.isInterrupted()) {
            Thread.sleep(1);
        }
        assertFalse(threadFinished.get());
        countDownLatch.countDown();
        thread.join();
        assertTrue(threadFinished.get());
    }

    @Test
    public void testWaitFor() throws Exception {
        try {
            SoakUtil.waitFor(1, 2, new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return false;
                }

                @Override
                public String toString() {
                    return "Godot";
                }
            });
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().startsWith("Timed out waiting for Godot"));
        }

        final AtomicInteger timesCalled = new AtomicInteger(0);
        SoakUtil.waitFor(1, 60000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return timesCalled.getAndIncrement() == 0;
            }

            @Override
            public String toString() {
                return "Incrementer";
            }
        });
        assertEquals(1, timesCalled.get());
    }

    @Test
    public void testMergeConfig() {
        HashMap<String, String> map1 = new HashMap<>();
        map1.put("foo", "1");
        map1.put("bar", "1");
        HashMap<String, String> map2 = new HashMap<>();
        map2.put("foo", "2");
        map2.put("quux", "2");
        Map<String, String> map3 = SoakUtil.mergeConfig(map1, map2);
        assertEquals("1", map3.get("foo"));
        assertEquals("1", map3.get("bar"));
        assertEquals("2", map3.get("quux"));
    }
};
