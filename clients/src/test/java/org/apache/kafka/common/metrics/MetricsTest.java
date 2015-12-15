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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetricsTest {

    private static final double EPS = 0.000001;
    private MockTime time = new MockTime();
    private MetricConfig config = new MetricConfig();
    private Metrics metrics;

    @Before
    public void setup() {
        this.metrics = new Metrics(config, Arrays.asList((MetricsReporter) new JmxReporter()), time, true);
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    @Test
    public void testMetricName() {
        MetricName n1 = metrics.metricName("name", "group", "description", "key1", "value1", "key2", "value2");
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("key1", "value1");
        tags.put("key2", "value2");
        MetricName n2 = metrics.metricName("name", "group", "description", tags);
        assertEquals("metric names created in two different ways should be equal", n1, n2);

        try {
            metrics.metricName("name", "group", "description", "key1");
            fail("Creating MetricName with an odd number of keyValue should fail");
        } catch (IllegalArgumentException e) {
            // this is expected
        }
    }

    @Test
    public void testSimpleStats() throws Exception {
        ConstantMeasurable measurable = new ConstantMeasurable();

        metrics.addMetric(metrics.metricName("direct.measurable", "grp1", "The fraction of time an appender waits for space allocation."), measurable);
        Sensor s = metrics.sensor("test.sensor");
        s.add(metrics.metricName("test.avg", "grp1"), new Avg());
        s.add(metrics.metricName("test.max", "grp1"), new Max());
        s.add(metrics.metricName("test.min", "grp1"), new Min());
        s.add(metrics.metricName("test.rate", "grp1"), new Rate(TimeUnit.SECONDS));
        s.add(metrics.metricName("test.occurences", "grp1"), new Rate(TimeUnit.SECONDS, new Count()));
        s.add(metrics.metricName("test.count", "grp1"), new Count());
        s.add(new Percentiles(100, -100, 100, BucketSizing.CONSTANT,
                             new Percentile(metrics.metricName("test.median", "grp1"), 50.0),
                             new Percentile(metrics.metricName("test.perc99_9", "grp1"), 99.9)));

        Sensor s2 = metrics.sensor("test.sensor2");
        s2.add(metrics.metricName("s2.total", "grp1"), new Total());
        s2.record(5.0);

        int sum = 0;
        int count = 10;
        for (int i = 0; i < count; i++) {
            s.record(i);
            sum += i;
        }
        // prior to any time passing
        double elapsedSecs = (config.timeWindowMs() * (config.samples() - 1)) / 1000.0;
        assertEquals(String.format("Occurrences(0...%d) = %f", count, count / elapsedSecs), count / elapsedSecs,
                     metrics.metrics().get(metrics.metricName("test.occurences", "grp1")).value(), EPS);

        // pretend 2 seconds passed...
        long sleepTimeMs = 2;
        time.sleep(sleepTimeMs * 1000);
        elapsedSecs += sleepTimeMs;

        assertEquals("s2 reflects the constant value", 5.0, metrics.metrics().get(metrics.metricName("s2.total", "grp1")).value(), EPS);
        assertEquals("Avg(0...9) = 4.5", 4.5, metrics.metrics().get(metrics.metricName("test.avg", "grp1")).value(), EPS);
        assertEquals("Max(0...9) = 9", count - 1, metrics.metrics().get(metrics.metricName("test.max", "grp1")).value(), EPS);
        assertEquals("Min(0...9) = 0", 0.0, metrics.metrics().get(metrics.metricName("test.min", "grp1")).value(), EPS);
        assertEquals("Rate(0...9) = 1.40625",
                     sum / elapsedSecs, metrics.metrics().get(metrics.metricName("test.rate", "grp1")).value(), EPS);
        assertEquals(String.format("Occurrences(0...%d) = %f", count, count / elapsedSecs),
                     count / elapsedSecs,
                     metrics.metrics().get(metrics.metricName("test.occurences", "grp1")).value(), EPS);
        assertEquals("Count(0...9) = 10",
                     (double) count, metrics.metrics().get(metrics.metricName("test.count", "grp1")).value(), EPS);
    }

    @Test
    public void testHierarchicalSensors() {
        Sensor parent1 = metrics.sensor("test.parent1");
        parent1.add(metrics.metricName("test.parent1.count", "grp1"), new Count());
        Sensor parent2 = metrics.sensor("test.parent2");
        parent2.add(metrics.metricName("test.parent2.count", "grp1"), new Count());
        Sensor child1 = metrics.sensor("test.child1", parent1, parent2);
        child1.add(metrics.metricName("test.child1.count", "grp1"), new Count());
        Sensor child2 = metrics.sensor("test.child2", parent1);
        child2.add(metrics.metricName("test.child2.count", "grp1"), new Count());
        Sensor grandchild = metrics.sensor("test.grandchild", child1);
        grandchild.add(metrics.metricName("test.grandchild.count", "grp1"), new Count());

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
        assertEquals(1.0 + gc, c1, EPS);
        assertEquals(1.0, c2, EPS);
        assertEquals(1.0 + c1, p2, EPS);
        assertEquals(1.0 + c1 + c2, p1, EPS);
        assertEquals(Arrays.asList(child1, child2), metrics.childrenSensors().get(parent1));
        assertEquals(Arrays.asList(child1), metrics.childrenSensors().get(parent2));
        assertNull(metrics.childrenSensors().get(grandchild));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadSensorHierarchy() {
        Sensor p = metrics.sensor("parent");
        Sensor c1 = metrics.sensor("child1", p);
        Sensor c2 = metrics.sensor("child2", p);
        metrics.sensor("gc", c1, c2); // should fail
    }

    @Test
    public void testRemoveSensor() {
        int size = metrics.metrics().size();
        Sensor parent1 = metrics.sensor("test.parent1");
        parent1.add(metrics.metricName("test.parent1.count", "grp1"), new Count());
        Sensor parent2 = metrics.sensor("test.parent2");
        parent2.add(metrics.metricName("test.parent2.count", "grp1"), new Count());
        Sensor child1 = metrics.sensor("test.child1", parent1, parent2);
        child1.add(metrics.metricName("test.child1.count", "grp1"), new Count());
        Sensor child2 = metrics.sensor("test.child2", parent2);
        child2.add(metrics.metricName("test.child2.count", "grp1"), new Count());
        Sensor grandChild1 = metrics.sensor("test.gchild2", child2);
        grandChild1.add(metrics.metricName("test.gchild2.count", "grp1"), new Count());

        Sensor sensor = metrics.getSensor("test.parent1");
        assertNotNull(sensor);
        metrics.removeSensor("test.parent1");
        assertNull(metrics.getSensor("test.parent1"));
        assertNull(metrics.metrics().get(metrics.metricName("test.parent1.count", "grp1")));
        assertNull(metrics.getSensor("test.child1"));
        assertNull(metrics.childrenSensors().get(sensor));
        assertNull(metrics.metrics().get(metrics.metricName("test.child1.count", "grp1")));

        sensor = metrics.getSensor("test.gchild2");
        assertNotNull(sensor);
        metrics.removeSensor("test.gchild2");
        assertNull(metrics.getSensor("test.gchild2"));
        assertNull(metrics.childrenSensors().get(sensor));
        assertNull(metrics.metrics().get(metrics.metricName("test.gchild2.count", "grp1")));

        sensor = metrics.getSensor("test.child2");
        assertNotNull(sensor);
        metrics.removeSensor("test.child2");
        assertNull(metrics.getSensor("test.child2"));
        assertNull(metrics.childrenSensors().get(sensor));
        assertNull(metrics.metrics().get(metrics.metricName("test.child2.count", "grp1")));

        sensor = metrics.getSensor("test.parent2");
        assertNotNull(sensor);
        metrics.removeSensor("test.parent2");
        assertNull(metrics.getSensor("test.parent2"));
        assertNull(metrics.childrenSensors().get(sensor));
        assertNull(metrics.metrics().get(metrics.metricName("test.parent2.count", "grp1")));

        assertEquals(size, metrics.metrics().size());
    }

    @Test
    public void testRemoveInactiveMetrics() {
        Sensor s1 = metrics.sensor("test.s1", null, 1);
        s1.add(metrics.metricName("test.s1.count", "grp1"), new Count());

        Sensor s2 = metrics.sensor("test.s2", null, 3);
        s2.add(metrics.metricName("test.s2.count", "grp1"), new Count());

        Metrics.ExpireSensorTask purger = metrics.new ExpireSensorTask();
        purger.run();
        assertNotNull("Sensor test.s1 must be present", metrics.getSensor("test.s1"));
        assertNotNull("MetricName test.s1.count must be present",
                metrics.metrics().get(metrics.metricName("test.s1.count", "grp1")));
        assertNotNull("Sensor test.s2 must be present", metrics.getSensor("test.s2"));
        assertNotNull("MetricName test.s2.count must be present",
                metrics.metrics().get(metrics.metricName("test.s2.count", "grp1")));

        time.sleep(1001);
        purger.run();
        assertNull("Sensor test.s1 should have been purged", metrics.getSensor("test.s1"));
        assertNull("MetricName test.s1.count should have been purged",
                metrics.metrics().get(metrics.metricName("test.s1.count", "grp1")));
        assertNotNull("Sensor test.s2 must be present", metrics.getSensor("test.s2"));
        assertNotNull("MetricName test.s2.count must be present",
                metrics.metrics().get(metrics.metricName("test.s2.count", "grp1")));

        // record a value in sensor s2. This should reset the clock for that sensor.
        // It should not get purged at the 3 second mark after creation
        s2.record();
        time.sleep(2000);
        purger.run();
        assertNotNull("Sensor test.s2 must be present", metrics.getSensor("test.s2"));
        assertNotNull("MetricName test.s2.count must be present",
                metrics.metrics().get(metrics.metricName("test.s2.count", "grp1")));

        // After another 1 second sleep, the metric should be purged
        time.sleep(1000);
        purger.run();
        assertNull("Sensor test.s2 should have been purged", metrics.getSensor("test.s1"));
        assertNull("MetricName test.s2.count should have been purged",
                metrics.metrics().get(metrics.metricName("test.s1.count", "grp1")));

        // After purging, it should be possible to recreate a metric
        s1 = metrics.sensor("test.s1", null, 1);
        s1.add(metrics.metricName("test.s1.count", "grp1"), new Count());
        assertNotNull("Sensor test.s1 must be present", metrics.getSensor("test.s1"));
        assertNotNull("MetricName test.s1.count must be present",
                metrics.metrics().get(metrics.metricName("test.s1.count", "grp1")));
    }

    @Test
    public void testRemoveMetric() {
        int size = metrics.metrics().size();
        metrics.addMetric(metrics.metricName("test1", "grp1"), new Count());
        metrics.addMetric(metrics.metricName("test2", "grp1"), new Count());

        assertNotNull(metrics.removeMetric(metrics.metricName("test1", "grp1")));
        assertNull(metrics.metrics().get(metrics.metricName("test1", "grp1")));
        assertNotNull(metrics.metrics().get(metrics.metricName("test2", "grp1")));

        assertNotNull(metrics.removeMetric(metrics.metricName("test2", "grp1")));
        assertNull(metrics.metrics().get(metrics.metricName("test2", "grp1")));

        assertEquals(size, metrics.metrics().size());
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
        metrics.sensor("test").add(metrics.metricName("test", "grp1"), new Avg());
        metrics.sensor("test2").add(metrics.metricName("test", "grp1"), new Total());
    }

    @Test
    public void testQuotas() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(metrics.metricName("test1.total", "grp1"), new Total(), new MetricConfig().quota(Quota.upperBound(5.0)));
        sensor.add(metrics.metricName("test2.total", "grp1"), new Total(), new MetricConfig().quota(Quota.lowerBound(0.0)));
        sensor.record(5.0);
        try {
            sensor.record(1.0);
            fail("Should have gotten a quota violation.");
        } catch (QuotaViolationException e) {
            // this is good
        }
        assertEquals(6.0, metrics.metrics().get(metrics.metricName("test1.total", "grp1")).value(), EPS);
        sensor.record(-6.0);
        try {
            sensor.record(-1.0);
            fail("Should have gotten a quota violation.");
        } catch (QuotaViolationException e) {
            // this is good
        }
    }

    @Test
    public void testQuotasEquality() {
        final Quota quota1 = Quota.upperBound(10.5);
        final Quota quota2 = Quota.lowerBound(10.5);

        assertFalse("Quota with different upper values shouldn't be equal", quota1.equals(quota2));

        final Quota quota3 = Quota.lowerBound(10.5);

        assertTrue("Quota with same upper and bound values should be equal", quota2.equals(quota3));
    }

    @Test
    public void testPercentiles() {
        int buckets = 100;
        Percentiles percs = new Percentiles(4 * buckets,
                                            0.0,
                                            100.0,
                                            BucketSizing.CONSTANT,
                                            new Percentile(metrics.metricName("test.p25", "grp1"), 25),
                                            new Percentile(metrics.metricName("test.p50", "grp1"), 50),
                                            new Percentile(metrics.metricName("test.p75", "grp1"), 75));
        MetricConfig config = new MetricConfig().eventWindow(50).samples(2);
        Sensor sensor = metrics.sensor("test", config);
        sensor.add(percs);
        Metric p25 = this.metrics.metrics().get(metrics.metricName("test.p25", "grp1"));
        Metric p50 = this.metrics.metrics().get(metrics.metricName("test.p50", "grp1"));
        Metric p75 = this.metrics.metrics().get(metrics.metricName("test.p75", "grp1"));

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

    @Test
    public void testRateWindowing() throws Exception {
        // Use the default time window. Set 3 samples
        MetricConfig cfg = new MetricConfig().samples(3);
        Sensor s = metrics.sensor("test.sensor", cfg);
        s.add(metrics.metricName("test.rate", "grp1"), new Rate(TimeUnit.SECONDS));

        int sum = 0;
        int count = cfg.samples() - 1;
        // Advance 1 window after every record
        for (int i = 0; i < count; i++) {
            s.record(100);
            sum += 100;
            time.sleep(cfg.timeWindowMs());
        }

        // Sleep for half the window.
        time.sleep(cfg.timeWindowMs() / 2);

        // prior to any time passing
        double elapsedSecs = (cfg.timeWindowMs() * (cfg.samples() - 1) + cfg.timeWindowMs() / 2) / 1000.0;

        KafkaMetric km = metrics.metrics().get(metrics.metricName("test.rate", "grp1"));
        assertEquals("Rate(0...2) = 2.666", sum / elapsedSecs, km.value(), EPS);
        assertEquals("Elapsed Time = 75 seconds", elapsedSecs,
                ((Rate) km.measurable()).windowSize(cfg, time.milliseconds()) / 1000, EPS);
    }

    public static class ConstantMeasurable implements Measurable {
        public double value = 0.0;

        @Override
        public double measure(MetricConfig config, long now) {
            return value;
        }

    }

}
