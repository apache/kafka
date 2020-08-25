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
package org.apache.kafka.common.metrics;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.stats.TokenBucket;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;

public class TokenBucketTest {
    Time time;

    @Before
    public void setup() {
        time = new MockTime(0, System.currentTimeMillis(), System.nanoTime());
    }

    @Test
    public void testRecord() {
        // Rate  = 5 unit / sec
        // Burst = 2 * 10 = 20 units
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(5))
            .timeWindow(2, TimeUnit.SECONDS)
            .samples(10);

        TokenBucket tk = new TokenBucket();

        // Expect 100 credits at T
        assertEquals(100, tk.measure(config, time.milliseconds()), 0.1);

        // Record 60 at T, expect 13 credits
        tk.record(config, 60, time.milliseconds());
        assertEquals(40, tk.measure(config, time.milliseconds()), 0.1);

        // Advance by 2s, record 5, expect 45 credits
        time.sleep(2000);
        tk.record(config, 5, time.milliseconds());
        assertEquals(45, tk.measure(config, time.milliseconds()), 0.1);

        // Advance by 2s, record 60, expect -5 credits
        time.sleep(2000);
        tk.record(config, 60, time.milliseconds());
        assertEquals(-5, tk.measure(config, time.milliseconds()), 0.1);
    }

    @Test
    public void testUnrecord() {
        // Rate  = 5 unit / sec
        // Burst = 2 * 10 = 20 units
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(5))
            .timeWindow(2, TimeUnit.SECONDS)
            .samples(10);

        TokenBucket tk = new TokenBucket();

        // Expect 100 credits at T
        assertEquals(100, tk.measure(config, time.milliseconds()), 0.1);

        // Record -60 at T, expect 100 credits
        tk.record(config, -60, time.milliseconds());
        assertEquals(100, tk.measure(config, time.milliseconds()), 0.1);

        // Advance by 2s, record 60, expect 40 credits
        time.sleep(2000);
        tk.record(config, 60, time.milliseconds());
        assertEquals(40, tk.measure(config, time.milliseconds()), 0.1);

        // Advance by 2s, record -60, expect 100 credits
        time.sleep(2000);
        tk.record(config, -60, time.milliseconds());
        assertEquals(100, tk.measure(config, time.milliseconds()), 0.1);
    }
}
