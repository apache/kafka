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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadFactory;

public class ThreadUtilsTest {

    private static final Runnable EMPTY_RUNNABLE = new Runnable() {
        @Override
        public void run() {
        }
    };
    private static final String THREAD_NAME = "ThreadName";
    private static final String THREAD_NAME_WITH_NUMBER = THREAD_NAME + "%d";


    @Test
    public void testThreadNameWithoutNumberNoDemon() {
        Assert.assertEquals(ThreadUtils.createThreadFactory(THREAD_NAME, false).
                newThread(EMPTY_RUNNABLE).getName(), THREAD_NAME);
    }

    @Test
    public void testThreadNameWithoutNumberDemon() {
        Thread daemonThread = ThreadUtils.createThreadFactory(THREAD_NAME, true).newThread(EMPTY_RUNNABLE);
        try {
            Assert.assertEquals(daemonThread.getName(), THREAD_NAME);
            Assert.assertTrue(daemonThread.isDaemon());
        } finally {
            try {
                daemonThread.join();
            } catch (InterruptedException e) {
                // can be ignored
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testThreadNameWithNumberNoDemon() {
        ThreadFactory localThreadFactory = ThreadUtils.createThreadFactory(THREAD_NAME_WITH_NUMBER, false);
        Assert.assertEquals(localThreadFactory.newThread(EMPTY_RUNNABLE).getName(), THREAD_NAME + "1");
        Assert.assertEquals(localThreadFactory.newThread(EMPTY_RUNNABLE).getName(), THREAD_NAME + "2");
    }

    @Test
    public void testThreadNameWithNumberDemon() {
        ThreadFactory localThreadFactory = ThreadUtils.createThreadFactory(THREAD_NAME_WITH_NUMBER, true);
        Thread daemonThread1 = localThreadFactory.newThread(EMPTY_RUNNABLE);
        Thread daemonThread2 = localThreadFactory.newThread(EMPTY_RUNNABLE);

        try {
            Assert.assertEquals(daemonThread1.getName(), THREAD_NAME + "1");
            Assert.assertTrue(daemonThread1.isDaemon());
        } finally {
            try {
                daemonThread1.join();
            } catch (InterruptedException e) {
                // can be ignored
                e.printStackTrace();
            }
        }
        try {
            Assert.assertEquals(daemonThread2.getName(), THREAD_NAME + "2");
            Assert.assertTrue(daemonThread2.isDaemon());
        } finally {
            try {
                daemonThread2.join();
            } catch (InterruptedException e) {
                // can be ignored
                e.printStackTrace();
            }
        }
    }
}
