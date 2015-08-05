/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.stream;

import org.apache.kafka.stream.util.ParallelExecutor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParallelExecutorTest {

    @Test
    public void testExecutingShortTaskList() throws Exception {
        ParallelExecutor parallelExecutor = new ParallelExecutor(10);
        ArrayList<TestTask> taskList = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            taskList.add(new TestTask(counter));
        }

        parallelExecutor.execute(taskList);

        for (TestTask task : taskList) {
            assertEquals(task.executionCount, 1);
        }
        assertEquals(counter.get(), taskList.size());

        parallelExecutor.execute(taskList);

        for (TestTask task : taskList) {
            assertEquals(task.executionCount, 2);
        }
        assertEquals(counter.get(), taskList.size() * 2);
    }

    @Test
    public void testExecutingLongTaskList() throws Exception {
        ParallelExecutor parallelExecutor = new ParallelExecutor(10);
        ArrayList<TestTask> taskList = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 20; i++) {
            taskList.add(new TestTask(counter));
        }

        parallelExecutor.execute(taskList);

        for (TestTask task : taskList) {
            assertEquals(task.executionCount, 1);
        }
        assertEquals(counter.get(), taskList.size());

        parallelExecutor.execute(taskList);

        for (TestTask task : taskList) {
            assertEquals(task.executionCount, 2);
        }
        assertEquals(counter.get(), taskList.size() * 2);
    }

    @Test
    public void testException() {
        ParallelExecutor parallelExecutor = new ParallelExecutor(10);
        ArrayList<TestTask> taskList = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 20; i++) {
            if (i == 15) {
                taskList.add(new TestTask(counter) {
                    @Override
                    public boolean process() {
                        throw new TestException();
                    }
                });
            } else {
                taskList.add(new TestTask(counter));
            }
        }

        Exception exception = null;
        try {
            parallelExecutor.execute(taskList);
        } catch (Exception ex) {
            exception = ex;
        }

        assertEquals(counter.get(), taskList.size() - 1);
        assertFalse(exception == null);
        assertTrue(exception instanceof TestException);
    }

    private static class TestTask implements ParallelExecutor.Task {
        public volatile int executionCount = 0;
        private AtomicInteger counter;

        TestTask(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public boolean process() {
            try {
                Thread.sleep(20);
                executionCount++;
            } catch (Exception ex) {
                // ignore
            }
            counter.incrementAndGet();

            return true;
        }
    }

    private static class TestException extends RuntimeException {
    }
}
