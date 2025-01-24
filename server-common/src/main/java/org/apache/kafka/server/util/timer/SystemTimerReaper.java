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

import org.apache.kafka.server.util.ShutdownableThread;

/**
 * SystemTimerReaper wraps a {@link Timer} and starts a reaper thread
 * to expire the tasks in the {@link Timer}.
 */
public class SystemTimerReaper implements Timer {
    private static final long WORK_TIMEOUT_MS = 200L;

    class Reaper extends ShutdownableThread {
        Reaper(String name) {
            super(name, false);
        }

        @Override
        public void doWork() {
            try {
                timer.advanceClock(WORK_TIMEOUT_MS);
            } catch (InterruptedException ex) {
                // Ignore.
            }
        }
    }

    private final Timer timer;
    private final Reaper reaper;

    public SystemTimerReaper(String reaperThreadName, Timer timer) {
        this.timer = timer;
        this.reaper = new Reaper(reaperThreadName);
        this.reaper.start();
    }

    @Override
    public void add(TimerTask timerTask) {
        timer.add(timerTask);
    }

    @Override
    public boolean advanceClock(long timeoutMs) throws InterruptedException {
        return timer.advanceClock(timeoutMs);
    }

    @Override
    public int size() {
        return timer.size();
    }

    @Override
    public void close() throws Exception {
        reaper.initiateShutdown();
        // Improve shutdown time by waking up the reaper thread
        // blocked on poll by sending a no-op.
        timer.add(new TimerTask(0) {
            @Override
            public void run() {}
        });
        reaper.awaitShutdown();
        timer.close();
    }

    // visible for testing
    boolean isShutdown() {
        return reaper.isShutdownComplete();
    }
}
