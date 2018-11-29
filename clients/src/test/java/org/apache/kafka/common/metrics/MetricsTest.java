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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.SimpleRate;
import org.apache.kafka.common.metrics.stats.Sum;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class MetricsTest {
    private static final Logger log = LoggerFactory.getLogger(MetricsTest.class);

    private static final double EPS = 0.000001;
    private MockTime time = new MockTime();
    private MetricConfig config = new MetricConfig();
    private Metrics metrics;
    private ExecutorService executorService;

    @Before
    public void setup() {
        this.metrics = new Metrics(config, Arrays.asList((MetricsReporter) new JmxReporter()), time, true);
    }

    @After
    public void tearDown() throws Exception {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
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
        verifyStats(m -> (double) m.metricValue());
    }

    private void verifyStats(Function<KafkaMetric, Double> metricValueFunc) {
        ConstantMeasurable measurable = new ConstantMeasurable();

        metrics.addMetric(metrics.metricName("direct.measurable", "grp1", "The fraction of time an appender waits for space allocation."), measurable);
        Sensor s = metrics.sensor("test.sensor");
        s.add(metrics.metricName("test.avg", "grp1"), new Avg());
        s.add(metrics.metricName("test.max", "grp1"), new Max());
        s.add(metrics.metricName("test.min", "grp1"), new Min());
        s.add(new Meter(TimeUnit.SECONDS, metrics.metricName("test.rate", "grp1"),
                metrics.metricName("test.total", "grp1")));
        s.add(new Meter(TimeUnit.SECONDS, new Count(), metrics.metricName("test.occurences", "grp1"),
                metrics.metricName("test.occurences.total", "grp1")));
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
                     metricValueFunc.apply(metrics.metrics().get(metrics.metricName("test.occurences", "grp1"))), EPS);

        // pretend 2 seconds passed...
        long sleepTimeMs = 2;
        time.sleep(sleepTimeMs * 1000);
        elapsedSecs += sleepTimeMs;

        assertEquals("s2 reflects the constant value", 5.0, metricValueFunc.apply(metrics.metrics().get(metrics.metricName("s2.total", "grp1"))), EPS);
        assertEquals("Avg(0...9) = 4.5", 4.5, metricValueFunc.apply(metrics.metrics().get(metrics.metricName("test.avg", "grp1"))), EPS);
        assertEquals("Max(0...9) = 9", count - 1,  metricValueFunc.apply(metrics.metrics().get(metrics.metricName("test.max", "grp1"))), EPS);
        assertEquals("Min(0...9) = 0", 0.0, metricValueFunc.apply(metrics.metrics().get(metrics.metricName("test.min", "grp1"))), EPS);
        assertEquals("Rate(0...9) = 1.40625",
                     sum / elapsedSecs, metricValueFunc.apply(metrics.metrics().get(metrics.metricName("test.rate", "grp1"))), EPS);
        assertEquals(String.format("Occurrences(0...%d) = %f", count, count / elapsedSecs),
                     count / elapsedSecs,
                     metricValueFunc.apply(metrics.metrics().get(metrics.metricName("test.occurences", "grp1"))), EPS);
        assertEquals("Count(0...9) = 10",
                     (double) count, metricValueFunc.apply(metrics.metrics().get(metrics.metricName("test.count", "grp1"))), EPS);
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

        double p1 = (double) parent1.metrics().get(0).metricValue();
        double p2 = (double) parent2.metrics().get(0).metricValue();
        double c1 = (double) child1.metrics().get(0).metricValue();
        double c2 = (double) child2.metrics().get(0).metricValue();
        double gc = (double) grandchild.metrics().get(0).metricValue();

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
        count.record(config, 1.0, time.absoluteMilliseconds());
        count.record(config, 1.0, time.absoluteMilliseconds());
        assertEquals(2.0, count.measure(config, time.absoluteMilliseconds()), EPS);
        count.record(config, 1.0, time.absoluteMilliseconds()); // first event times out
        assertEquals(2.0, count.measure(config, time.absoluteMilliseconds()), EPS);
    }

    @Test
    public void testTimeWindowing() {
        Count count = new Count();
        MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.MILLISECONDS).samples(2);
        count.record(config, 1.0, time.absoluteMilliseconds());
        time.sleep(1);
        count.record(config, 1.0, time.absoluteMilliseconds());
        assertEquals(2.0, count.measure(config, time.absoluteMilliseconds()), EPS);
        time.sleep(1);
        count.record(config, 1.0, time.absoluteMilliseconds()); // oldest event times out
        assertEquals(2.0, count.measure(config, time.absoluteMilliseconds()), EPS);
    }

    @Test
    public void testOldDataHasNoEffect() {
        Max max = new Max();
        long windowMs = 100;
        int samples = 2;
        MetricConfig config = new MetricConfig().timeWindow(windowMs, TimeUnit.MILLISECONDS).samples(samples);
        max.record(config, 50, time.absoluteMilliseconds());
        time.sleep(samples * windowMs);
        assertEquals(Double.NaN, max.measure(config, time.absoluteMilliseconds()), EPS);
    }

    /**
     * Some implementations of SampledStat make sense to return NaN
     * when there are no values set rather than the initial value
     */
    @Test
    public void testSampledStatReturnsNaNWhenNoValuesExist() {
        // This is tested by having a SampledStat with expired Stats,
        // because their values get reset to the initial values.
        Max max = new Max();
        Min min = new Min();
        Avg avg = new Avg();
        long windowMs = 100;
        int samples = 2;
        MetricConfig config = new MetricConfig().timeWindow(windowMs, TimeUnit.MILLISECONDS).samples(samples);
        max.record(config, 50, time.absoluteMilliseconds());
        min.record(config, 50, time.absoluteMilliseconds());
        avg.record(config, 50, time.absoluteMilliseconds());

        time.sleep(samples * windowMs);

        assertEquals(Double.NaN, max.measure(config, time.absoluteMilliseconds()), EPS);
        assertEquals(Double.NaN, min.measure(config, time.absoluteMilliseconds()), EPS);
        assertEquals(Double.NaN, avg.measure(config, time.absoluteMilliseconds()), EPS);
    }

    /**
     * Some implementations of SampledStat make sense to return the initial value
     * when there are no values set
     */
    @Test
    public void testSampledStatReturnsInitialValueWhenNoValuesExist() {
        Count count = new Count();
        Rate.SampledTotal sampledTotal = new Rate.SampledTotal();
        long windowMs = 100;
        int samples = 2;
        MetricConfig config = new MetricConfig().timeWindow(windowMs, TimeUnit.MILLISECONDS).samples(samples);

        count.record(config, 50, time.absoluteMilliseconds());
        sampledTotal.record(config, 50, time.absoluteMilliseconds());

        time.sleep(samples * windowMs);

        assertEquals(0, count.measure(config, time.absoluteMilliseconds()), EPS);
        assertEquals(0.0, sampledTotal.measure(config, time.absoluteMilliseconds()), EPS);
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
        assertEquals(6.0, (Double) metrics.metrics().get(metrics.metricName("test1.total", "grp1")).metricValue(), EPS);
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

        assertEquals(25, (Double) p25.metricValue(), 1.0);
        assertEquals(50, (Double) p50.metricValue(), 1.0);
        assertEquals(75, (Double) p75.metricValue(), 1.0);

        for (int i = 0; i < buckets; i++)
            sensor.record(0.0);

        assertEquals(0.0, (Double) p25.metricValue(), 1.0);
        assertEquals(0.0, (Double) p50.metricValue(), 1.0);
        assertEquals(0.0, (Double) p75.metricValue(), 1.0);

        // record two more windows worth of sequential values
        for (int i = 0; i < buckets; i++)
            sensor.record(i);

        assertEquals(25, (Double) p25.metricValue(), 1.0);
        assertEquals(50, (Double) p50.metricValue(), 1.0);
        assertEquals(75, (Double) p75.metricValue(), 1.0);
    }

    @Test
    public void testRateWindowing() throws Exception {
        // Use the default time window. Set 3 samples
        MetricConfig cfg = new MetricConfig().samples(3);
        Sensor s = metrics.sensor("test.sensor", cfg);
        MetricName rateMetricName = metrics.metricName("test.rate", "grp1");
        MetricName totalMetricName = metrics.metricName("test.total", "grp1");
        MetricName countRateMetricName = metrics.metricName("test.count.rate", "grp1");
        MetricName countTotalMetricName = metrics.metricName("test.count.total", "grp1");
        s.add(new Meter(TimeUnit.SECONDS, rateMetricName, totalMetricName));
        s.add(new Meter(TimeUnit.SECONDS, new Count(), countRateMetricName, countTotalMetricName));
        KafkaMetric totalMetric = metrics.metrics().get(totalMetricName);
        KafkaMetric countTotalMetric = metrics.metrics().get(countTotalMetricName);

        int sum = 0;
        int count = cfg.samples() - 1;
        // Advance 1 window after every record
        for (int i = 0; i < count; i++) {
            s.record(100);
            sum += 100;
            time.sleep(cfg.timeWindowMs());
            assertEquals(sum, (Double) totalMetric.metricValue(), EPS);
        }

        // Sleep for half the window.
        time.sleep(cfg.timeWindowMs() / 2);

        // prior to any time passing
        double elapsedSecs = (cfg.timeWindowMs() * (cfg.samples() - 1) + cfg.timeWindowMs() / 2) / 1000.0;

        KafkaMetric rateMetric = metrics.metrics().get(rateMetricName);
        KafkaMetric countRateMetric = metrics.metrics().get(countRateMetricName);
        assertEquals("Rate(0...2) = 2.666", sum / elapsedSecs, (Double) rateMetric.metricValue(), EPS);
        assertEquals("Count rate(0...2) = 0.02666", count / elapsedSecs, (Double) countRateMetric.metricValue(), EPS);
        assertEquals("Elapsed Time = 75 seconds", elapsedSecs,
                ((Rate) rateMetric.measurable()).windowSize(cfg, time.absoluteMilliseconds()) / 1000, EPS);
        assertEquals(sum, (Double) totalMetric.metricValue(), EPS);
        assertEquals(count, (Double) countTotalMetric.metricValue(), EPS);

        // Verify that rates are expired, but total is cumulative
        time.sleep(cfg.timeWindowMs() * cfg.samples());
        assertEquals(0, (Double) rateMetric.metricValue(), EPS);
        assertEquals(0, (Double) countRateMetric.metricValue(), EPS);
        assertEquals(sum, (Double) totalMetric.metricValue(), EPS);
        assertEquals(count, (Double) countTotalMetric.metricValue(), EPS);
    }

    public static class ConstantMeasurable implements Measurable {
        public double value = 0.0;

        @Override
        public double measure(MetricConfig config, long now) {
            return value;
        }

    }

    @Test
    public void testSimpleRate() {
        SimpleRate rate = new SimpleRate();

        //Given
        MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.SECONDS).samples(10);

        //In the first window the rate is a fraction of the whole (1s) window
        //So when we record 1000 at t0, the rate should be 1000 until the window completes, or more data is recorded.
        record(rate, config, 1000);
        assertEquals(1000, measure(rate, config), 0);
        time.sleep(100);
        assertEquals(1000, measure(rate, config), 0); // 1000B / 0.1s
        time.sleep(100);
        assertEquals(1000, measure(rate, config), 0); // 1000B / 0.2s
        time.sleep(200);
        assertEquals(1000, measure(rate, config), 0); // 1000B / 0.4s

        //In the second (and subsequent) window(s), the rate will be in proportion to the elapsed time
        //So the rate will degrade over time, as the time between measurement and the initial recording grows.
        time.sleep(600);
        assertEquals(1000, measure(rate, config), 0); // 1000B / 1.0s
        time.sleep(200);
        assertEquals(1000 / 1.2, measure(rate, config), 0); // 1000B / 1.2s
        time.sleep(200);
        assertEquals(1000 / 1.4, measure(rate, config), 0); // 1000B / 1.4s

        //Adding another value, inside the same window should double the rate
        record(rate, config, 1000);
        assertEquals(2000 / 1.4, measure(rate, config), 0); // 2000B / 1.4s

        //Going over the next window, should not change behaviour
        time.sleep(1100);
        assertEquals(2000 / 2.5, measure(rate, config), 0); // 2000B / 2.5s
        record(rate, config, 1000);
        assertEquals(3000 / 2.5, measure(rate, config), 0); // 3000B / 2.5s

        //Sleeping for another 6.5 windows also should be the same
        time.sleep(6500);
        assertEquals(3000 / 9, measure(rate, config), 1); // 3000B / 9s
        record(rate, config, 1000);
        assertEquals(4000 / 9, measure(rate, config), 1); // 4000B / 9s

        //Going over the 10 window boundary should cause the first window's values (1000) will be purged.
        //So the rate is calculated based on the oldest reading, which is inside the second window, at 1.4s
        time.sleep(1500);
        assertEquals((4000 - 1000) / (10.5 - 1.4), measure(rate, config), 1);
        record(rate, config, 1000);
        assertEquals((5000 - 1000) / (10.5 - 1.4), measure(rate, config), 1);
    }

    private void record(Rate rate, MetricConfig config, int value) {
        rate.record(config, value, time.absoluteMilliseconds());
    }

    private Double measure(Measurable rate, MetricConfig config) {
        return rate.measure(config, time.absoluteMilliseconds());
    }
    
    @Test
    public void testMetricInstances() {
        MetricName n1 = metrics.metricInstance(SampleMetrics.METRIC1, "key1", "value1", "key2", "value2");
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("key1", "value1");
        tags.put("key2", "value2");
        MetricName n2 = metrics.metricInstance(SampleMetrics.METRIC2, tags);
        assertEquals("metric names created in two different ways should be equal", n1, n2);

        try {
            metrics.metricInstance(SampleMetrics.METRIC1, "key1");
            fail("Creating MetricName with an odd number of keyValue should fail");
        } catch (IllegalArgumentException e) {
            // this is expected
        }
        
        Map<String, String> parentTagsWithValues = new HashMap<>();
        parentTagsWithValues.put("parent-tag", "parent-tag-value");

        Map<String, String> childTagsWithValues = new HashMap<>();
        childTagsWithValues.put("child-tag", "child-tag-value");

        try (Metrics inherited = new Metrics(new MetricConfig().tags(parentTagsWithValues), Arrays.asList((MetricsReporter) new JmxReporter()), time, true)) {
            MetricName inheritedMetric = inherited.metricInstance(SampleMetrics.METRIC_WITH_INHERITED_TAGS, childTagsWithValues);

            Map<String, String> filledOutTags = inheritedMetric.tags();
            assertEquals("parent-tag should be set properly", filledOutTags.get("parent-tag"), "parent-tag-value");
            assertEquals("child-tag should be set properly", filledOutTags.get("child-tag"), "child-tag-value");

            try {
                inherited.metricInstance(SampleMetrics.METRIC_WITH_INHERITED_TAGS, parentTagsWithValues);
                fail("Creating MetricName should fail if the child metrics are not defined at runtime");
            } catch (IllegalArgumentException e) {
                // this is expected
            }

            try {

                Map<String, String> runtimeTags = new HashMap<>();
                runtimeTags.put("child-tag", "child-tag-value");
                runtimeTags.put("tag-not-in-template", "unexpected-value");

                inherited.metricInstance(SampleMetrics.METRIC_WITH_INHERITED_TAGS, runtimeTags);
                fail("Creating MetricName should fail if there is a tag at runtime that is not in the template");
            } catch (IllegalArgumentException e) {
                // this is expected
            }
        }
    }

    /**
     * Verifies that concurrent sensor add, remove, updates and read don't result
     * in errors or deadlock.
     */
    @Test
    public void testConcurrentReadUpdate() throws Exception {
        final Random random = new Random();
        final Deque<Sensor> sensors = new ConcurrentLinkedDeque<>();
        metrics = new Metrics(new MockTime(10));
        SensorCreator sensorCreator = new SensorCreator(metrics);

        final AtomicBoolean alive = new AtomicBoolean(true);
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new ConcurrentMetricOperation(alive, "record",
            () -> sensors.forEach(sensor -> sensor.record(random.nextInt(10000)))));

        for (int i = 0; i < 10000; i++) {
            if (sensors.size() > 5) {
                Sensor sensor = random.nextBoolean() ? sensors.removeFirst() : sensors.removeLast();
                metrics.removeSensor(sensor.name());
            }
            StatType statType = StatType.forId(random.nextInt(StatType.values().length));
            sensors.add(sensorCreator.createSensor(statType, i));
            for (Sensor sensor : sensors) {
                for (KafkaMetric metric : sensor.metrics()) {
                    assertNotNull("Invalid metric value", metric.metricValue());
                }
            }
        }
        alive.set(false);
    }

    /**
     * Verifies that concurrent sensor add, remove, updates and read with a metrics reporter
     * that synchronizes on every reporter method doesn't result in errors or deadlock.
     */
    @Test
    public void testConcurrentReadUpdateReport() throws Exception {

        class LockingReporter implements MetricsReporter {
            Map<MetricName, KafkaMetric> activeMetrics = new HashMap<>();
            @Override
            public synchronized void init(List<KafkaMetric> metrics) {
            }

            @Override
            public synchronized void metricChange(KafkaMetric metric) {
                activeMetrics.put(metric.metricName(), metric);
            }

            @Override
            public synchronized void metricRemoval(KafkaMetric metric) {
                activeMetrics.remove(metric.metricName(), metric);
            }

            @Override
            public synchronized void close() {
            }

            @Override
            public void configure(Map<String, ?> configs) {
            }

            synchronized void processMetrics() {
                for (KafkaMetric metric : activeMetrics.values()) {
                    assertNotNull("Invalid metric value", metric.metricValue());
                }
            }
        }

        final LockingReporter reporter = new LockingReporter();
        this.metrics.close();
        this.metrics = new Metrics(config, Arrays.asList((MetricsReporter) reporter), new MockTime(10), true);
        final Deque<Sensor> sensors = new ConcurrentLinkedDeque<>();
        SensorCreator sensorCreator = new SensorCreator(metrics);

        final Random random = new Random();
        final AtomicBoolean alive = new AtomicBoolean(true);
        executorService = Executors.newFixedThreadPool(3);

        Future<?> writeFuture = executorService.submit(new ConcurrentMetricOperation(alive, "record",
            () -> sensors.forEach(sensor -> sensor.record(random.nextInt(10000)))));
        Future<?> readFuture = executorService.submit(new ConcurrentMetricOperation(alive, "read",
            () -> sensors.forEach(sensor -> sensor.metrics().forEach(metric ->
                assertNotNull("Invalid metric value", metric.metricValue())))));
        Future<?> reportFuture = executorService.submit(new ConcurrentMetricOperation(alive, "report",
            () -> reporter.processMetrics()));

        for (int i = 0; i < 10000; i++) {
            if (sensors.size() > 10) {
                Sensor sensor = random.nextBoolean() ? sensors.removeFirst() : sensors.removeLast();
                metrics.removeSensor(sensor.name());
            }
            StatType statType = StatType.forId(random.nextInt(StatType.values().length));
            sensors.add(sensorCreator.createSensor(statType, i));
        }
        assertFalse("Read failed", readFuture.isDone());
        assertFalse("Write failed", writeFuture.isDone());
        assertFalse("Report failed", reportFuture.isDone());

        alive.set(false);
    }

    private class ConcurrentMetricOperation implements Runnable {
        private final AtomicBoolean alive;
        private final String opName;
        private final Runnable op;
        ConcurrentMetricOperation(AtomicBoolean alive, String opName, Runnable op) {
            this.alive = alive;
            this.opName = opName;
            this.op = op;
        }
        public void run() {
            try {
                while (alive.get()) {
                    op.run();
                }
            } catch (Throwable t) {
                log.error("Metric {} failed with exception", opName, t);
            }
        }
    }

    enum StatType {
        AVG(0),
        TOTAL(1),
        COUNT(2),
        MAX(3),
        MIN(4),
        RATE(5),
        SIMPLE_RATE(6),
        SUM(7),
        VALUE(8),
        PERCENTILES(9),
        METER(10);

        int id;
        StatType(int id) {
            this.id = id;
        }

        static StatType forId(int id) {
            for (StatType statType : StatType.values()) {
                if (statType.id == id)
                    return statType;
            }
            return null;
        }
    }

    private static class SensorCreator {

        private final Metrics metrics;

        SensorCreator(Metrics metrics) {
            this.metrics = metrics;
        }

        private Sensor createSensor(StatType statType, int index) {
            Sensor sensor = metrics.sensor("kafka.requests." + index);
            Map<String, String> tags = Collections.singletonMap("tag", "tag" + index);
            switch (statType) {
                case AVG:
                    sensor.add(metrics.metricName("test.metric.avg", "avg", tags), new Avg());
                    break;
                case TOTAL:
                    sensor.add(metrics.metricName("test.metric.total", "total", tags), new Total());
                    break;
                case COUNT:
                    sensor.add(metrics.metricName("test.metric.count", "count", tags), new Count());
                    break;
                case MAX:
                    sensor.add(metrics.metricName("test.metric.max", "max", tags), new Max());
                    break;
                case MIN:
                    sensor.add(metrics.metricName("test.metric.min", "min", tags), new Min());
                    break;
                case RATE:
                    sensor.add(metrics.metricName("test.metric.rate", "rate", tags), new Rate());
                    break;
                case SIMPLE_RATE:
                    sensor.add(metrics.metricName("test.metric.simpleRate", "simpleRate", tags), new SimpleRate());
                    break;
                case SUM:
                    sensor.add(metrics.metricName("test.metric.sum", "sum", tags), new Sum());
                    break;
                case VALUE:
                    sensor.add(metrics.metricName("test.metric.value", "value", tags), new Value());
                    break;
                case PERCENTILES:
                    sensor.add(metrics.metricName("test.metric.percentiles", "percentiles", tags),
                               new Percentiles(100, -100, 100, Percentiles.BucketSizing.CONSTANT,
                                               new Percentile(metrics.metricName("test.median", "percentiles"), 50.0),
                                               new Percentile(metrics.metricName("test.perc99_9", "percentiles"), 99.9)));
                    break;
                case METER:
                    sensor.add(new Meter(metrics.metricName("test.metric.meter.rate", "meter", tags),
                               metrics.metricName("test.metric.meter.total", "meter", tags)));
                    break;
                default:
                    throw new IllegalStateException("Invalid stat type " + statType);
            }
            return sensor;
        }
    }

    /**
     * This test is to verify the deprecated {@link Metric#value()} method.
     * @deprecated This will be removed in a future major release.
     */
    @Deprecated
    @Test
    public void testDeprecatedMetricValueMethod() {
        verifyStats(KafkaMetric::value);
    }
}
