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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class TimeTest {

    protected abstract Time createTime();

    @Test
    public void testWaitObjectTimeout() throws InterruptedException {
        Object obj = new Object();
        Time time = createTime();
        long timeoutMs = 100;
        long deadlineMs = time.milliseconds() + timeoutMs;
        AtomicReference<Exception> caughtException = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                time.waitObject(obj, () -> false, deadlineMs);
            } catch (Exception e) {
                caughtException.set(e);
            }
        });

        t.start();
        time.sleep(timeoutMs);
        t.join();

        assertEquals(TimeoutException.class, caughtException.get().getClass());
    }

    @Test
    public void testWaitObjectConditionSatisfied() throws InterruptedException {
        Object obj = new Object();
        Time time = createTime();
        long timeoutMs = 1000000000;
        long deadlineMs = time.milliseconds() + timeoutMs;
        AtomicBoolean condition = new AtomicBoolean(false);
        AtomicReference<Exception> caughtException = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                time.waitObject(obj, condition::get, deadlineMs);
            } catch (Exception e) {
                caughtException.set(e);
            }
        });

        t.start();

        synchronized (obj) {
            condition.set(true);
            obj.notify();
        }

        t.join();

        assertTrue(time.milliseconds() < deadlineMs);
        assertNull(caughtException.get());
    }
}
