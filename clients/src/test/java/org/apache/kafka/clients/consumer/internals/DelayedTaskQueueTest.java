/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class DelayedTaskQueueTest {
    private DelayedTaskQueue scheduler = new DelayedTaskQueue();
    private ArrayList<DelayedTask> executed = new ArrayList<DelayedTask>();

    @Test
    public void testScheduling() {
        // Empty scheduler
        assertEquals(Long.MAX_VALUE, scheduler.nextTimeout(0));
        scheduler.poll(0);
        assertEquals(Collections.emptyList(), executed);

        TestTask task1 = new TestTask();
        TestTask task2 = new TestTask();
        TestTask task3 = new TestTask();
        scheduler.add(task1, 20);
        assertEquals(20, scheduler.nextTimeout(0));
        scheduler.add(task2, 10);
        assertEquals(10, scheduler.nextTimeout(0));
        scheduler.add(task3, 30);
        assertEquals(10, scheduler.nextTimeout(0));

        scheduler.poll(5);
        assertEquals(Collections.emptyList(), executed);
        assertEquals(5, scheduler.nextTimeout(5));

        scheduler.poll(10);
        assertEquals(Arrays.asList(task2), executed);
        assertEquals(10, scheduler.nextTimeout(10));

        scheduler.poll(20);
        assertEquals(Arrays.asList(task2, task1), executed);
        assertEquals(20, scheduler.nextTimeout(10));

        scheduler.poll(30);
        assertEquals(Arrays.asList(task2, task1, task3), executed);
        assertEquals(Long.MAX_VALUE, scheduler.nextTimeout(30));
    }

    @Test
    public void testRemove() {
        TestTask task1 = new TestTask();
        TestTask task2 = new TestTask();
        TestTask task3 = new TestTask();
        scheduler.add(task1, 20);
        scheduler.add(task2, 10);
        scheduler.add(task3, 30);
        scheduler.add(task1, 40);
        assertEquals(10, scheduler.nextTimeout(0));

        scheduler.remove(task2);
        assertEquals(20, scheduler.nextTimeout(0));

        scheduler.remove(task1);
        assertEquals(30, scheduler.nextTimeout(0));

        scheduler.remove(task3);
        assertEquals(Long.MAX_VALUE, scheduler.nextTimeout(0));
    }

    private class TestTask implements DelayedTask {
        @Override
        public void run(long now) {
            executed.add(this);
        }
    }

}
