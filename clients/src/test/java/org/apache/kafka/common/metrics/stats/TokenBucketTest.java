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
package org.apache.kafka.common.metrics.stats;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TokenBucketTest {
    Time time;

    @Before
    public void setup() {
        time = new MockTime(0, 0, 0);
    }

    @Test
    public void testOneSampleIsDecreasedCorrectly() {
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(1))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        TokenBucket tk = new TokenBucket();

        // Record 6 at T
        tk.record(config, 6, time.milliseconds());

        // Expect 6 at T
        assertEquals(6, tk.measure(config, time.milliseconds()), 0.1);

        // Expect 4 at T+2s
        time.sleep(2000);
        assertEquals(4, tk.measure(config, time.milliseconds()), 0.1);

        // Expect 2 at T+4s
        time.sleep(2000);
        assertEquals(2, tk.measure(config, time.milliseconds()), 0.1);

        // Expect 0 at T+6s
        time.sleep(2000);
        assertEquals(0, tk.measure(config, time.milliseconds()), 0.1);
    }

    @Test
    public void testOneSampleIsDecreasedCorrectlyAndThenDropped() {
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(1))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        TokenBucket tk = new TokenBucket();

        // Record 20 at T
        tk.record(config, 20, time.milliseconds());

        // Expect 10 at T+10s
        time.sleep(10000);
        assertEquals(10, tk.measure(config, time.milliseconds()), 0.1);

        // Expect 0 at T+11s -> The sample has been dropped
        time.sleep(1000);
        assertEquals(0, tk.measure(config, time.milliseconds()), 0.1);
    }

    @Test
    public void testMultipleSamples() {
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(5))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        TokenBucket tk = new TokenBucket();

        // Record 50 (S1) at T
        tk.record(config, 50, time.milliseconds());

        // Expect 50 at T
        assertEquals(50, tk.measure(config, time.milliseconds()), 0.1);

        // Record 50 (S2) at T+5s
        time.sleep(5000);
        tk.record(config, 50, time.milliseconds());

        // Expect 75 at T+5s
        // Current Samples:
        // T+00s => 50
        // T+05s => 50
        // Intermediate Total:
        // T+00s => 50
        // T+05s => (50-5*5) + 50 = 75
        assertEquals(75, tk.measure(config, time.milliseconds()), 0.1);

        // Record 50 (S3) at T+10s
        time.sleep(5000);
        tk.record(config, 50, time.milliseconds());

        // Expect 100 at T+10s
        // Current Samples:
        // T+00s => 50
        // T+05s => 50
        // T+10s => 50
        // Intermediate Total:
        // T+00s => 50
        // T+05s => (50-5*5) + 50 = 75
        // T+10s => (75-5*5) + 50 = 100
        assertEquals(100, tk.measure(config, time.milliseconds()), 0.1);

        // Record 50 (S4) at T+15s
        time.sleep(5000);
        tk.record(config, 50, time.milliseconds());

        // Expect 100 at T+15s
        // Current Samples:
        // T+05s => 50
        // T+10s => 50
        // T+15s => 50
        // Intermediate Total:
        // T+05s => 50
        // T+10s => (50-5*5) + 50 = 75
        // T+15s => (75-5*5) + 50 = 100
        assertEquals(100, tk.measure(config, time.milliseconds()), 0.1);

        // Expect 50 at T+20s
        // Current Samples:
        // T+10s => 50
        // T+15s => 50
        // Intermediate Total:
        // T+10s => 50
        // T+15s => (50-5*5) + 50 = 75
        // T+20s => 75-5*5 = 50
        time.sleep(5000);
        assertEquals(50, tk.measure(config, time.milliseconds()), 0.1);
    }

    @Test
    public void testAdjustedTotalDoesNotGoBelowZero() {
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(4))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        TokenBucket tk = new TokenBucket();

        // Record 2 at T
        tk.record(config, 2, time.milliseconds());

        // Expect 2 at T
        assertEquals(2, tk.measure(config, time.milliseconds()), 0.1);

        // Record 4 at T+1s
        time.sleep(1000);
        tk.record(config, 4, time.milliseconds());

        // Expect 4 at T+1s
        // By having 4, we ensure that the adjusted total did not go to -2
        assertEquals(4, tk.measure(config, time.milliseconds()), 0.1);
    }

    @Test
    public void testNegativeSampleDoesNotBringTotalBelowZero() {
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(5))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        TokenBucket tk = new TokenBucket();

        // Record 50 at T
        tk.record(config, 50, time.milliseconds());

        // Expect 50 at T
        assertEquals(50, tk.measure(config, time.milliseconds()), 0.1);

        // Expect 50 at T+5s
        time.sleep(5000);
        assertEquals(25, tk.measure(config, time.milliseconds()), 0.1);

        // Payback 50 at T+5s
        tk.record(config, -50, time.milliseconds());

        // Expect 0 at T+5s
        assertEquals(0, tk.measure(config, time.milliseconds()), 0.1);
    }
}
