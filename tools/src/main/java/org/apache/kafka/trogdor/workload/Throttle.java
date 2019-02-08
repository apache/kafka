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

package org.apache.kafka.trogdor.workload;

import org.apache.kafka.common.utils.Time;

public class Throttle {
    private final int maxPerPeriod;
    private final int periodMs;
    private int count;
    private long prevPeriod;
    private long lastTimeMs;

    Throttle(int maxPerPeriod, int periodMs) {
        this.maxPerPeriod = maxPerPeriod;
        this.periodMs = periodMs;
        this.count = maxPerPeriod;
        this.prevPeriod = -1;
        this.lastTimeMs = 0;
    }

    synchronized public boolean increment() throws InterruptedException {
        boolean throttled = false;
        while (true) {
            if (count < maxPerPeriod) {
                count++;
                return throttled;
            }
            lastTimeMs = time().milliseconds();
            long curPeriod = lastTimeMs / periodMs;
            if (curPeriod <= prevPeriod) {
                long nextPeriodMs = (curPeriod + 1) * periodMs;
                delay(nextPeriodMs - lastTimeMs);
                throttled = true;
            } else {
                prevPeriod = curPeriod;
                count = 0;
            }
        }
    }

    public synchronized long lastTimeMs() {
        return lastTimeMs;
    }

    protected Time time() {
        return Time.SYSTEM;
    }

    protected synchronized void delay(long amount) throws InterruptedException {
        if (amount > 0) {
            wait(amount);
        }
    }
}
