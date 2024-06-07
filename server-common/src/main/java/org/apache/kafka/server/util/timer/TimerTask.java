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

public abstract class TimerTask implements Runnable {
    private volatile TimerTaskEntry timerTaskEntry;
    // timestamp in millisecond
    public final long delayMs;

    public TimerTask(long delayMs) {
        this.delayMs = delayMs;
    }

    public void cancel() {
        synchronized (this) {
            if (timerTaskEntry != null) timerTaskEntry.remove();
            timerTaskEntry = null;
        }
    }

    final void setTimerTaskEntry(TimerTaskEntry entry) {
        synchronized (this) {
            // if this timerTask is already held by an existing timer task entry,
            // we will remove such an entry first.
            if (timerTaskEntry != null && timerTaskEntry != entry) {
                timerTaskEntry.remove();
            }

            timerTaskEntry = entry;
        }
    }

    TimerTaskEntry getTimerTaskEntry() {
        return timerTaskEntry;
    }
}
