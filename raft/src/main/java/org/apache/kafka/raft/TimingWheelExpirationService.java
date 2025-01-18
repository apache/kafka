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
package org.apache.kafka.raft;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.server.util.ShutdownableThread;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import java.util.concurrent.CompletableFuture;

public class TimingWheelExpirationService implements ExpirationService {

    private static final long WORK_TIMEOUT_MS = 200L;

    private final ExpiredOperationReaper expirationReaper;
    private final Timer timer;

    public TimingWheelExpirationService(Timer timer) {
        this.timer = timer;
        this.expirationReaper = new ExpiredOperationReaper();
        expirationReaper.start();
    }

    @Override
    public <T> CompletableFuture<T> failAfter(long timeoutMs) {
        TimerTaskCompletableFuture<T> task = new TimerTaskCompletableFuture<>(timeoutMs);
        task.future.whenComplete((t, throwable) -> task.cancel());
        timer.add(task);
        return task.future;
    }

    public void shutdown() throws InterruptedException {
        expirationReaper.shutdown();
    }

    private static class TimerTaskCompletableFuture<T> extends TimerTask {

        private final CompletableFuture<T> future = new CompletableFuture<>();

        TimerTaskCompletableFuture(long delayMs) {
            super(delayMs);
        }

        @Override
        public void run() {
            future.completeExceptionally(new TimeoutException("Future failed to be completed before timeout of " + delayMs + " ms was reached"));
        }
    }

    private class ExpiredOperationReaper extends ShutdownableThread {

        ExpiredOperationReaper() {
            super("raft-expiration-reaper", false);
        }

        @Override
        public void doWork() {
            try {
                timer.advanceClock(WORK_TIMEOUT_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
