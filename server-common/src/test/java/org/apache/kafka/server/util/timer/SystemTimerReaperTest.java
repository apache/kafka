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

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;

public class SystemTimerReaperTest {
    private static class FutureTimerTask<T> extends TimerTask {
        CompletableFuture<T> future = new CompletableFuture<>();

        FutureTimerTask(long delayMs) {
            super(delayMs);
        }

        @Override
        public void run() {
            // We use org.apache.kafka.common.errors.TimeoutException to differentiate
            // from java.util.concurrent.TimeoutException.
            future.completeExceptionally(new TimeoutException(
                String.format("Future failed to be completed before timeout of %sMs ms was reached", delayMs)));
        }
    }

    private <T> CompletableFuture<T> add(Timer timer, long delayMs) {
        FutureTimerTask<T> task = new FutureTimerTask<>(delayMs);
        timer.add(task);
        return task.future;
    }

    @Test
    public void testReaper() throws Exception {
        try (Timer timer = new SystemTimerReaper("reaper", new SystemTimer("timer"))) {
            CompletableFuture<Void> t1 = add(timer, 100L);
            CompletableFuture<Void> t2 = add(timer, 200L);
            CompletableFuture<Void> t3 = add(timer, 300L);
            TestUtils.assertFutureThrows(t1, TimeoutException.class);
            TestUtils.assertFutureThrows(t2, TimeoutException.class);
            TestUtils.assertFutureThrows(t3, TimeoutException.class);
        }
    }

    @Test
    public void testReaperClose() throws Exception {
        Timer timer = Mockito.mock(Timer.class);
        SystemTimerReaper timerReaper = new SystemTimerReaper("reaper", timer);
        timerReaper.close();
        Mockito.verify(timer, Mockito.times(1)).close();
        TestUtils.waitForCondition(timerReaper::isShutdown, "reaper not shutdown");
    }
}
