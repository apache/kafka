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

import java.util.Collection;

/**
 * Simple RateLimiter in terms of records-per-second.
 *
 */
public class RecordRateLimiter<R extends ConnectRecord> implements RateLimiter<R> {

    private final double targetRate;
    private Time time;
    private long prevTime;
    private int accumulatedRecordCount = 0;

    public RecordRateLimiter(double targetRate) {
        this.targetRate = targetRate;
    }

    @Override
    public void start(Time time) {
        this.time = time;
        this.prevTime = time.milliseconds();
    }

    @Override
    public void accumulate(Collection<R> records) {
        // Since we're only rate-limiting in terms of records-per-second in this impl, we only
        // care about the number of records, not their contents.
        accumulatedRecordCount += records.size();
    }
 
    @Override
    public void throttle() throws InterruptedException {
        long t = throttleTime();
        if (t > 0) {
            Thread.sleep(t);
        }
    }

    // Visible for testing
    long throttleTime() {
        // Elapsed time since last throttle
        long now = time.milliseconds();
        double elapsed = ((double) (now - prevTime)) / 1000.0;
        prevTime = now;

        // Observed rate during that interval
        double rate = (double) accumulatedRecordCount / elapsed;
        accumulatedRecordCount = 0;
    
        // How long to sleep, based on previous interval
        double rateError = (rate - targetRate) / targetRate;
        return (long) Math.max(elapsed * rateError, 0.0);
    }

    @Override
    public void close() {
    }
}
