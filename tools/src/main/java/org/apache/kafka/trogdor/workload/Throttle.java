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

import java.util.concurrent.TimeUnit;

public class Throttle {
    private final int maxPerSec;
    private int count;
    private long second;

    Throttle(int maxPerSec) {
        this.maxPerSec = maxPerSec;
    }

    synchronized public boolean increment() throws InterruptedException {
        boolean throttled = false;
        while (true) {
            if (count < maxPerSec) {
                count++;
                return throttled;
            }
            long now = time().milliseconds();
            long nextSecondMs = TimeUnit.MILLISECONDS.convert(second + 1, TimeUnit.SECONDS);
            if (now < nextSecondMs) {
                delay(nextSecondMs - now);
                throttled = true;
            }  else {
                second = TimeUnit.SECONDS.convert(now, TimeUnit.MILLISECONDS);
                count = 0;
            }
        }
    }

    protected Time time() {
        return Time.SYSTEM;
    }

    protected synchronized void delay(long amount) throws InterruptedException {
        wait(amount);
    }
}
