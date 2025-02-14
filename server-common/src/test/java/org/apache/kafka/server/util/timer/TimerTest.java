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
package org.apache.kafka.server.util.timer;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TimerTest {

    private static class TestTask extends TimerTask {
        final int id;
        final CountDownLatch latch;
        final List<Integer> output;
        final AtomicBoolean completed = new AtomicBoolean(false);

        TestTask(
            long delayMs,
            int id,
            CountDownLatch latch,
            List<Integer> output
        ) {
            super(delayMs);
            this.id = id;
            this.latch = latch;
            this.output = output;
        }

        @Override
        public void run() {
            if (completed.compareAndSet(false, true)) {
                synchronized (output) {
                    output.add(id);
                }
                latch.countDown();
            }
        }
    }

    private SystemTimer timer = null;

    @BeforeEach
    public void setup() {
        timer = new SystemTimer("test", 1, 3, Time.SYSTEM.hiResClockMs());
    }

    @AfterEach
    public void teardown() throws Exception {
        timer.close();
        TestUtils.waitForCondition(timer::isTerminated, "timer executor not terminated");
    }

    @Test
    public void testAlreadyExpiredTask() throws InterruptedException {
        List<Integer> output = new ArrayList<>();

        List<CountDownLatch> latches = IntStream.range(-5, 0).mapToObj(i -> {
            CountDownLatch latch = new CountDownLatch(1);
            timer.add(new TestTask(i, i, latch, output));
            return latch;
        }).toList();

        timer.advanceClock(0L);

        latches.stream().limit(5).forEach(latch -> {
            try {
                assertTrue(latch.await(3, TimeUnit.SECONDS),
                    "already expired tasks should run immediately");
            } catch (InterruptedException e) {
                fail("interrupted");
            }
        });

        assertEquals(Set.of(-5, -4, -3, -2, -1), new HashSet<>(output),
            "output of already expired tasks");
    }

    @Test
    public void testTaskExpiration() throws InterruptedException {
        List<Integer> output = new ArrayList<>();
        List<TestTask> tasks = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        List<CountDownLatch> latches = new ArrayList<>();

        IntStream.range(0, 5).forEach(i -> {
            CountDownLatch latch = new CountDownLatch(1);
            tasks.add(new TestTask(i, i, latch, output));
            ids.add(i);
            latches.add(latch);
        });

        IntStream.range(10, 100).forEach(i -> {
            CountDownLatch latch = new CountDownLatch(2);
            tasks.add(new TestTask(i, i, latch, output));
            tasks.add(new TestTask(i, i, latch, output));
            ids.add(i);
            ids.add(i);
            latches.add(latch);
        });

        IntStream.range(100, 500).forEach(i -> {
            CountDownLatch latch = new CountDownLatch(1);
            tasks.add(new TestTask(i, i, latch, output));
            ids.add(i);
            latches.add(latch);
        });

        // randomly submit requests
        tasks.forEach(task -> timer.add(task));

        while (timer.advanceClock(2000)) { }

        latches.forEach(latch -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail("interrupted");
            }
        });

        assertEquals(ids, output.stream().sorted().toList(),
            "output should match");
    }
}
