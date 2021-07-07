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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.RateLimiter;

/**
 * Simple RateLimiter in terms of records-per-second.
 *
 */
public abstract class CountingRateLimiter<R extends ConnectRecord> implements RateLimiter<R> {

    private double targetRate = -1;
    private Time time;
    private long prevTime;
    private int accumulatedCount = 0;

    protected void setTargetRate(double targetRate) {
        this.targetRate = targetRate;
    }

    protected void count(int n) {
        accumulatedCount += n;
    }

    @Override
    public void start(Time time) {
        this.time = time;
        this.prevTime = time.milliseconds();
    }

    @Override
    public long throttleTime() {
        if (targetRate < 0) {
            return 0L;
        }

        // Elapsed time since last throttle
        long now = time.milliseconds();
        double elapsed = ((double) (now - prevTime)) / 1000.0;
        prevTime = now;

        // Observed rate during that interval
        double rate = (double) accumulatedCount / elapsed;
        accumulatedCount = 0;
    
        // How long to sleep, based on previous interval
        double rateError = (rate - targetRate) / targetRate;
        return (long) Math.max(elapsed * rateError, 0.0);
    }

    @Override
    public void close() {
    }
}
