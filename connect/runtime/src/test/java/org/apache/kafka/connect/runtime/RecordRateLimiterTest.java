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

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.common.utils.MockTime;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;

public class RecordRateLimiterTest {

    @Test
    public void testHighTargetRate() {
        RecordRateLimiter<SinkRecord> limiter = new RecordRateLimiter<>(10_000);
        MockTime time = new MockTime();
        limiter.start(time);

        limiter.accumulate(nRecords(0));
        time.sleep(10000);                      // 10 seconds
        assertEquals("Don't throttle when zero records are sent",
            0L, limiter.throttleTime());
 
        limiter.accumulate(nRecords(1000));     // well under the max we should see in 10 seconds
        time.sleep(10000);                      // 10 seconds
        assertEquals("Don't throttle when much less than the target rate is sent",
            0L, limiter.throttleTime());
 
        limiter.accumulate(nRecords(99999));    // barely under the max we should see in 10 seconds
        time.sleep(10000);                      // 10 seconds
        assertEquals("Don't throttle when barely less than the target rate is sent",
            0L, limiter.throttleTime());
    
        limiter.accumulate(nRecords(200_000));  // well over the max we should see in 10 seconds
        time.sleep(10000);                      // 10 seconds
        assertTrue("Throttle when well over the target rate is sent",
            limiter.throttleTime() > 0L);
    
        limiter.accumulate(nRecords(1000001));  // barely over the max we should see in 10 seconds
        time.sleep(10000);                      // 10 seconds
        assertTrue("Throttle when barely over the target rate is sent",
            limiter.throttleTime() > 0L);
    }

    @Test
    public void testLowTargetRate() {
        RecordRateLimiter<SinkRecord> limiter = new RecordRateLimiter<>(1);
        MockTime time = new MockTime();
        limiter.start(time);

        limiter.accumulate(nRecords(0));
        time.sleep(10000);                      // 10 seconds
        assertEquals("Don't throttle when zero records are sent",
            0L, limiter.throttleTime());

        limiter.accumulate(nRecords(5));        // well under the max we should see in 10 seconds
        time.sleep(10000);                      // 10 seconds
        assertEquals("Don't throttle when much less than the target rate is sent",
            0L, limiter.throttleTime());
 
        limiter.accumulate(nRecords(9));        // barely under the max we should see in 10 seconds
        time.sleep(10000);                      // 10 seconds
        assertEquals("Don't throttle when barely less than the target rate is sent",
            0L, limiter.throttleTime());
    
        limiter.accumulate(nRecords(20));       // well over the max we should see in 10 seconds
        time.sleep(10000);                      // 10 seconds
        assertTrue("Throttle when well over the target rate is sent",
            limiter.throttleTime() > 0L);
    
        limiter.accumulate(nRecords(11));       // barely over the max we should see in 10 seconds
        time.sleep(10000);                      // 10 seconds
        assertTrue("Throttle when barely over the target rate is sent",
            limiter.throttleTime() > 0L);
    }
 
    private Collection<SinkRecord> nRecords(int n) {
        return Collections.nCopies(n, new SinkRecord("test", 0, null, null, null, null, 0));
    }
}
