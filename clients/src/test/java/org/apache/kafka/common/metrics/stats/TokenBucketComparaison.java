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
    private Time time;

    @Before
    public void setup() {
        time = new MockTime(0, 0, 0);
    }

    @Test
    public void compareOneBurstSmallerThanMaxBurst() {
        // Max Burst = 5 * 1 * 10 = 50
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(5))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        RealTokenBucket real = new RealTokenBucket();
        SampledTokenBucket sampled = new SampledTokenBucket();
        BackfillTokenBucket backfill = new BackfillTokenBucket();

        // Record 15 at T = 0s
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());


        // Expect 15 at T = 0s
        assertEquals(15, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(15, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(15, backfill.measure(config, time.milliseconds()), 0.1);

        // Expect 0 at T = 3s
        time.sleep(3000);
        assertEquals(0, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, backfill.measure(config, time.milliseconds()), 0.1);
    }

    @Test
    public void compareOneBurstEqualToMaxBurst() {
        // Max Burst = 5 * 1 * 10 = 50
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(5))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        RealTokenBucket real = new RealTokenBucket();
        SampledTokenBucket sampled = new SampledTokenBucket();
        BackfillTokenBucket backfill = new BackfillTokenBucket();

        // Record 50 at T = 0s
        real.record(config, 50, time.milliseconds());
        sampled.record(config, 50, time.milliseconds());
        backfill.record(config, 50, time.milliseconds());

        // Expect 50 at T = 0s
        assertEquals(50, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(50, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(50, backfill.measure(config, time.milliseconds()), 0.1);

        // Expect 0 at T = 10s
        time.sleep(10000);
        assertEquals(0, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, backfill.measure(config, time.milliseconds()), 0.1);
    }

    @Test
    public void compareOneBurstLargerThanMaxBurst() {
        // Max Burst = 5 * 1 * 10 = 50
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(5))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        RealTokenBucket real = new RealTokenBucket();
        SampledTokenBucket sampled = new SampledTokenBucket();
        BackfillTokenBucket backfill = new BackfillTokenBucket();

        // Record 200 at T = 0s
        real.record(config, 200, time.milliseconds());
        sampled.record(config, 200, time.milliseconds());
        backfill.record(config, 200, time.milliseconds());

        // Expect 200 at T = 0s
        assertEquals(200, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(200, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(200, backfill.measure(config, time.milliseconds()), 0.1);

        // Expect 50 at T = 10s
        time.sleep(10000);
        assertEquals(150, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(150, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(150, backfill.measure(config, time.milliseconds()), 0.1);

        // Expect 45 at T = 11s
        time.sleep(1000);
        assertEquals(145, real.measure(config, time.milliseconds()), 0.1);

        // /!\ /!\ /!\
        // Both implementations drop to 0 because all the samples have been
        // purged after 10s whereas the real implementation yields the correct
        // value.
        assertEquals(0, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(0, backfill.measure(config, time.milliseconds()), 0.1);
    }

    @Test
    public void compareMultipleBurstsSmallerThanMaxBurst() {
        // Max Burst = 5 * 1 * 10 = 50
        MetricConfig config = new MetricConfig()
            .quota(Quota.upperBound(5))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11);

        RealTokenBucket real = new RealTokenBucket();
        SampledTokenBucket sampled = new SampledTokenBucket();
        BackfillTokenBucket backfill = new BackfillTokenBucket();

        // Record 15 at T = 0s
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());

        // Expect 15 at T = 0s
        assertEquals(15, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(15, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(15, backfill.measure(config, time.milliseconds()), 0.1);

        // Record 15 at T = 2s
        time.sleep(2000);
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());

        // Expect 20 at T = 2s
        assertEquals(20, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(20, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(20, backfill.measure(config, time.milliseconds()), 0.1);

        // Record 15 at T = 4s
        time.sleep(2000);
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());

        // Expect 25 at T = 4s
        assertEquals(25, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(25, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(25, backfill.measure(config, time.milliseconds()), 0.1);

        // Record 15 at T = 6s
        time.sleep(2000);
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());

        // Expect 30 at T = 6s
        assertEquals(30, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(30, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(30, backfill.measure(config, time.milliseconds()), 0.1);

        // Record 15 at T = 8s
        time.sleep(2000);
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());

        // Expect 35 at T = 8s
        assertEquals(35, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(35, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(35, backfill.measure(config, time.milliseconds()), 0.1);

        // Record 15 at T = 10s
        time.sleep(2000);
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());

        // Expect 40 at T = 10s
        assertEquals(40, real.measure(config, time.milliseconds()), 0.1);
        assertEquals(40, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(40, backfill.measure(config, time.milliseconds()), 0.1);

        // Record 15 at T = 12s
        time.sleep(2000);
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());

        // Expect 45 at T = 12s
        assertEquals(45, real.measure(config, time.milliseconds()), 0.1);
        // /!\ /!\ /!\
        // The "sampled" implementation starts to differ from this point one because the
        // oldest sample has been purged.
        assertEquals(40, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(45, backfill.measure(config, time.milliseconds()), 0.1);

        // Record 15 at T = 14s
        time.sleep(2000);
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());

        // Expect 50 at T = 14s
        assertEquals(50, real.measure(config, time.milliseconds()), 0.1);
        // /!\ /!\ /!\
        assertEquals(40, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(50, backfill.measure(config, time.milliseconds()), 0.1);

        // Record 15 at T = 16s
        time.sleep(2000);
        real.record(config, 15, time.milliseconds());
        sampled.record(config, 15, time.milliseconds());
        backfill.record(config, 15, time.milliseconds());

        // Expect 55 at T = 16s
        assertEquals(55, real.measure(config, time.milliseconds()), 0.1);
        // /!\ /!\ /!\
        assertEquals(40, sampled.measure(config, time.milliseconds()), 0.1);
        assertEquals(55, backfill.measure(config, time.milliseconds()), 0.1);
    }
}
