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

public class TokenBucketComparaison {
    Time time;

    @Before
    public void setup() {
        time = new MockTime(0, 0, 0);
    }

    @Test
    public void compare() {
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(5))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        SampledTokenBucket sampledTokenBucket = new SampledTokenBucket();
        BackfillTokenBucket backfillTokenBucket = new BackfillTokenBucket();
        RealTokenBucket realTokenBucket = new RealTokenBucket();

        sampledTokenBucket.record(config, 50, time.milliseconds());
        backfillTokenBucket.record(config, 50, time.milliseconds());
        realTokenBucket.record(config, 50, time.milliseconds());

        assertEquals(50, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(50, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(50, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        time.sleep(5000);

        assertEquals(25, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(25, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(25, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        time.sleep(5000);

        assertEquals(0, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        time.sleep(5000);

        assertEquals(0, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        sampledTokenBucket.record(config, 50, time.milliseconds());
        backfillTokenBucket.record(config, 50, time.milliseconds());
        realTokenBucket.record(config, 50, time.milliseconds());

        assertEquals(50, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(50, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(50, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        time.sleep(5000);

        sampledTokenBucket.record(config, 50, time.milliseconds());
        backfillTokenBucket.record(config, 50, time.milliseconds());
        realTokenBucket.record(config, 50, time.milliseconds());

        assertEquals(75, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(75, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(75, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        time.sleep(5000);

        sampledTokenBucket.record(config, 50, time.milliseconds());
        backfillTokenBucket.record(config, 50, time.milliseconds());
        realTokenBucket.record(config, 50, time.milliseconds());

        assertEquals(100, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(100, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(100, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        time.sleep(5000);

        // Drop to 50 because first sample is dropped
        assertEquals(50, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(75, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(75, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        time.sleep(5000);

        // Drop to 0 because second sample is dropped
        assertEquals(0, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(50, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(50, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        time.sleep(5000);

        assertEquals(0, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        // Drop to 0 because last sample is dropped
        assertEquals(0, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(25, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        time.sleep(5000);

        assertEquals(0, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, realTokenBucket.measure(config, time.milliseconds()), 0.1);

        sampledTokenBucket.record(config, 500, time.milliseconds());
        backfillTokenBucket.record(config, 500, time.milliseconds());
        realTokenBucket.record(config, 500, time.milliseconds());

        for (int i = 1; i <= 10; i++) {
            time.sleep(1000);
            double expected = 500 - i * 5;
            assertEquals(expected, realTokenBucket.measure(config, time.milliseconds()), 0.1);
            assertEquals(expected, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
            assertEquals(expected, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
        }

        time.sleep(1000);
        assertEquals(445, realTokenBucket.measure(config, time.milliseconds()), 0.1);
        // Both drop to 0 because samples are expired
        assertEquals(0, backfillTokenBucket.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, sampledTokenBucket.measure(config, time.milliseconds()), 0.1);
    }
}
