/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

public class MetricsTest {

    private static double EPS = 0.000001;

    MockTime time = new MockTime();
    Metrics metrics = new Metrics(new MetricConfig(), Arrays.asList((MetricsReporter) new JmxReporter()), time);

    @Test
    public void testMetricName() {
        MetricName n1 = new MetricName("name", "group", "description", "key1", "value1");
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("key1", "value1");
        MetricName n2 = new MetricName("name", "group", "description", tags);
        assertEquals("metric names created in two different ways should be equal", n1, n2);

        try {
            new MetricName("name", "group", "description", "key1");
            fail("Creating MetricName with an old number of keyValue should fail");
        } catch (IllegalArgumentException e) {
            // this is expected
        }
    }

    @Test
    public void testSimpleStats() throws Exception {
        ConstantMeasurable measurable = new ConstantMeasurable();

        metrics.addMetric(new MetricName("direct.measurable", "grp1", "The fraction of time an appender waits for space allocation."), measurable);
        Sensor s = metrics.sensor("test.sensor");
        s.add(new MetricName("test.avg", "grp1"), new Avg());
        s.add(new MetricName("test.max", "grp1"), new Max());
        s.add(new MetricName("test.min", "grp1"), new Min());
        s.add(new MetricName("test.rate", "grp1"), new Rate(TimeUnit.SECONDS));
        s.add(new MetricName("test.occurences", "grp1"), new Rate(TimeUnit.SECONDS, new Count()));
        s.add(new MetricName("test.count", "grp1"), new Count());
        s.add(new Percentiles(100, -100, 100, BucketSizing.CONSTANT,
                             new Percentile(new MetricName("test.median", "grp1"), 50.0),
                             new Percentile(new MetricName("test.perc99_9", "grp1"),99.9)));

        Sensor s2 = metrics.sensor("test.sensor2");
        s2.add(new MetricName("s2.total", "grp1"), new Total());
        s2.record(5.0);

        for (int i = 0; i < 10; i++)
            s.record(i);

        // pretend 2 seconds passed...
        time.sleep(2000);

        assertEquals("s2 reflects the constant value", 5.0, metrics.metrics().get(new MetricName("s2.total", "grp1")).value(), EPS);
        assertEquals("Avg(0...9) = 4.5", 4.5, metrics.metrics().get(new MetricName("test.avg", "grp1")).value(), EPS);
        assertEquals("Max(0...9) = 9", 9.0, metrics.metrics().get(new MetricName("test.max", "grp1")).value(), EPS);
        assertEquals("Min(0...9) = 0", 0.0, metrics.metrics().get(new MetricName("test.min", "grp1")).value(), EPS);
        assertEquals("Rate(0...9) = 22.5", 22.5, metrics.metrics().get(new MetricName("test.rate", "grp1")).value(), EPS);
        assertEquals("Occurences(0...9) = 5", 5.0, metrics.metrics().get(new MetricName("test.occurences", "grp1")).value(), EPS);
        assertEquals("Count(0...9) = 10", 10.0, metrics.metrics().get(new MetricName("test.count", "grp1")).value(), EPS);
    }

    @Test
    public void testHierarchicalSensors() {
        Sensor parent1 = metrics.sensor("test.parent1");
        parent1.add(new MetricName("test.parent1.count", "grp1"), new Count());
        Sensor parent2 = metrics.sensor("test.parent2");
        parent2.add(new MetricName("test.parent2.count", "grp1"), new Count());
        Sensor child1 = metrics.sensor("test.child1", parent1, parent2);
        child1.add(new MetricName("test.child1.count", "grp1"), new Count());
        Sensor child2 = metrics.sensor("test.child2", parent1);
        child2.add(new MetricName("test.child2.count", "grp1"), new Count());
        Sensor grandchild = metrics.sensor("test.grandchild", child1);
        grandchild.add(new MetricName("test.grandchild.count", "grp1"), new Count());

        /* increment each sensor one time */
        parent1.record();
        parent2.record();
        child1.record();
        child2.record();
        grandchild.record();

        double p1 = parent1.metrics().get(0).value();
        double p2 = parent2.metrics().get(0).value();
        double c1 = child1.metrics().get(0).value();
        double c2 = child2.metrics().get(0).value();
        double gc = grandchild.metrics().get(0).value();

        /* each metric should have a count equal to one + its children's count */
        assertEquals(1.0, gc, EPS);
        assertEquals(1.0 + gc, child1.metrics().get(0).value(), EPS);
        assertEquals(1.0, c2, EPS);
        assertEquals(1.0 + c1, p2, EPS);
        assertEquals(1.0 + c1 + c2, p1, EPS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadSensorHiearchy() {
        Sensor p = metrics.sensor("parent");
        Sensor c1 = metrics.sensor("child1", p);
        Sensor c2 = metrics.sensor("child2", p);
        metrics.sensor("gc", c1, c2); // should fail
    }

    @Test
    public void testEventWindowing() {
        Count count = new Count();
        MetricConfig config = new MetricConfig().eventWindow(1).samples(2);
        count.record(config, 1.0, time.milliseconds());
        count.record(config, 1.0, time.milliseconds());
        assertEquals(2.0, count.measure(config, time.milliseconds()), EPS);
        count.record(config, 1.0, time.milliseconds()); // first event times out
        assertEquals(2.0, count.measure(config, time.milliseconds()), EPS);
    }

    @Test
    public void testTimeWindowing() {
        Count count = new Count();
        MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.MILLISECONDS).samples(2);
        count.record(config, 1.0, time.milliseconds());
        time.sleep(1);
        count.record(config, 1.0, time.milliseconds());
        assertEquals(2.0, count.measure(config, time.milliseconds()), EPS);
        time.sleep(1);
        count.record(config, 1.0, time.milliseconds()); // oldest event times out
        assertEquals(2.0, count.measure(config, time.milliseconds()), EPS);
    }

    @Test
    public void testOldDataHasNoEffect() {
        Max max = new Max();
        long windowMs = 100;
        int samples = 2;
        MetricConfig config = new MetricConfig().timeWindow(windowMs, TimeUnit.MILLISECONDS).samples(samples);
        max.record(config, 50, time.milliseconds());
        time.sleep(samples * windowMs);
        assertEquals(Double.NEGATIVE_INFINITY, max.measure(config, time.milliseconds()), EPS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateMetricName() {
        metrics.sensor("test").add(new MetricName("test", "grp1"), new Avg());
        metrics.sensor("test2").add(new MetricName("test", "grp1"), new Total());
    }

    @Test
    public void testQuotas() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(new MetricName("test1.total", "grp1"), new Total(), new MetricConfig().quota(Quota.lessThan(5.0)));
        sensor.add(new MetricName("test2.total", "grp1"), new Total(), new MetricConfig().quota(Quota.moreThan(0.0)));
        sensor.record(5.0);
        try {
            sensor.record(1.0);
            fail("Should have gotten a quota violation.");
        } catch (QuotaViolationException e) {
            // this is good
        }
        assertEquals(6.0, metrics.metrics().get(new MetricName("test1.total", "grp1")).value(), EPS);
        sensor.record(-6.0);
        try {
            sensor.record(-1.0);
            fail("Should have gotten a quota violation.");
        } catch (QuotaViolationException e) {
            // this is good
        }
    }

    @Test
    public void testPercentiles() {
        int buckets = 100;
        Percentiles percs = new Percentiles(4 * buckets,
                                            0.0,
                                            100.0,
                                            BucketSizing.CONSTANT,
                                            new Percentile(new MetricName("test.p25", "grp1"), 25),
                                            new Percentile(new MetricName("test.p50", "grp1"), 50),
                                            new Percentile(new MetricName("test.p75", "grp1"), 75));
        MetricConfig config = new MetricConfig().eventWindow(50).samples(2);
        Sensor sensor = metrics.sensor("test", config);
        sensor.add(percs);
        Metric p25 = this.metrics.metrics().get(new MetricName("test.p25", "grp1"));
        Metric p50 = this.metrics.metrics().get(new MetricName("test.p50", "grp1"));
        Metric p75 = this.metrics.metrics().get(new MetricName("test.p75", "grp1"));

        // record two windows worth of sequential values
        for (int i = 0; i < buckets; i++)
            sensor.record(i);

        assertEquals(25, p25.value(), 1.0);
        assertEquals(50, p50.value(), 1.0);
        assertEquals(75, p75.value(), 1.0);

        for (int i = 0; i < buckets; i++)
            sensor.record(0.0);

        assertEquals(0.0, p25.value(), 1.0);
        assertEquals(0.0, p50.value(), 1.0);
        assertEquals(0.0, p75.value(), 1.0);
    }

    public static class ConstantMeasurable implements Measurable {
        public double value = 0.0;

        @Override
        public double measure(MetricConfig config, long now) {
            return value;
        }

    }

}
