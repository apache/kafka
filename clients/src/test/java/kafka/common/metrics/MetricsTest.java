package kafka.common.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import kafka.common.Metric;
import kafka.common.metrics.stats.Avg;
import kafka.common.metrics.stats.Count;
import kafka.common.metrics.stats.Max;
import kafka.common.metrics.stats.Min;
import kafka.common.metrics.stats.Percentile;
import kafka.common.metrics.stats.Percentiles;
import kafka.common.metrics.stats.Percentiles.BucketSizing;
import kafka.common.metrics.stats.Rate;
import kafka.common.metrics.stats.Total;
import kafka.common.utils.MockTime;

import org.junit.Test;

public class MetricsTest {

    private static double EPS = 0.000001;

    MockTime time = new MockTime();
    Metrics metrics = new Metrics(new MetricConfig(), Arrays.asList((MetricsReporter) new JmxReporter()), time);

    @Test
    public void testSimpleStats() throws Exception {
        ConstantMeasurable measurable = new ConstantMeasurable();
        metrics.addMetric("direct.measurable", measurable);
        Sensor s = metrics.sensor("test.sensor");
        s.add("test.avg", new Avg());
        s.add("test.max", new Max());
        s.add("test.min", new Min());
        s.add("test.rate", new Rate(TimeUnit.SECONDS));
        s.add("test.occurences", new Rate(TimeUnit.SECONDS, new Count()));
        s.add("test.count", new Count());
        s.add(new Percentiles(100, -100, 100, BucketSizing.CONSTANT, new Percentile("test.median", 50.0), new Percentile("test.perc99_9",
                                                                                                                         99.9)));

        Sensor s2 = metrics.sensor("test.sensor2");
        s2.add("s2.total", new Total());
        s2.record(5.0);

        for (int i = 0; i < 10; i++)
            s.record(i);

        // pretend 2 seconds passed...
        time.sleep(2000);

        assertEquals("s2 reflects the constant value", 5.0, metrics.metrics().get("s2.total").value(), EPS);
        assertEquals("Avg(0...9) = 4.5", 4.5, metrics.metrics().get("test.avg").value(), EPS);
        assertEquals("Max(0...9) = 9", 9.0, metrics.metrics().get("test.max").value(), EPS);
        assertEquals("Min(0...9) = 0", 0.0, metrics.metrics().get("test.min").value(), EPS);
        assertEquals("Rate(0...9) = 22.5", 22.5, metrics.metrics().get("test.rate").value(), EPS);
        assertEquals("Occurences(0...9) = 5", 5.0, metrics.metrics().get("test.occurences").value(), EPS);
        assertEquals("Count(0...9) = 10", 10.0, metrics.metrics().get("test.count").value(), EPS);
    }

    @Test
    public void testHierarchicalSensors() {
        Sensor parent1 = metrics.sensor("test.parent1");
        parent1.add("test.parent1.count", new Count());
        Sensor parent2 = metrics.sensor("test.parent2");
        parent2.add("test.parent2.count", new Count());
        Sensor child1 = metrics.sensor("test.child1", parent1, parent2);
        child1.add("test.child1.count", new Count());
        Sensor child2 = metrics.sensor("test.child2", parent1);
        child2.add("test.child2.count", new Count());
        Sensor grandchild = metrics.sensor("test.grandchild", child1);
        grandchild.add("test.grandchild.count", new Count());

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
        count.record(config, 1.0, time.nanoseconds());
        count.record(config, 1.0, time.nanoseconds());
        assertEquals(2.0, count.measure(config, time.nanoseconds()), EPS);
        count.record(config, 1.0, time.nanoseconds()); // first event times out
        assertEquals(2.0, count.measure(config, time.nanoseconds()), EPS);
    }

    @Test
    public void testTimeWindowing() {
        Count count = new Count();
        MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.MILLISECONDS).samples(2);
        count.record(config, 1.0, time.nanoseconds());
        time.sleep(1);
        count.record(config, 1.0, time.nanoseconds());
        assertEquals(2.0, count.measure(config, time.nanoseconds()), EPS);
        time.sleep(1);
        count.record(config, 1.0, time.nanoseconds()); // oldest event times out
        assertEquals(2.0, count.measure(config, time.nanoseconds()), EPS);
    }

    @Test
    public void testOldDataHasNoEffect() {
        Max max = new Max();
        long windowMs = 100;
        MetricConfig config = new MetricConfig().timeWindow(windowMs, TimeUnit.MILLISECONDS);
        max.record(config, 50, time.nanoseconds());
        time.sleep(windowMs);
        assertEquals(Double.NEGATIVE_INFINITY, max.measure(config, time.nanoseconds()), EPS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateMetricName() {
        metrics.sensor("test").add("test", new Avg());
        metrics.sensor("test2").add("test", new Total());
    }

    @Test
    public void testQuotas() {
        Sensor sensor = metrics.sensor("test");
        sensor.add("test1.total", new Total(), new MetricConfig().quota(Quota.lessThan(5.0)));
        sensor.add("test2.total", new Total(), new MetricConfig().quota(Quota.moreThan(0.0)));
        sensor.record(5.0);
        try {
            sensor.record(1.0);
            fail("Should have gotten a quota violation.");
        } catch (QuotaViolationException e) {
            // this is good
        }
        assertEquals(6.0, metrics.metrics().get("test1.total").value(), EPS);
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
                                            new Percentile("test.p25", 25),
                                            new Percentile("test.p50", 50),
                                            new Percentile("test.p75", 75));
        MetricConfig config = new MetricConfig().eventWindow(50).samples(2);
        Sensor sensor = metrics.sensor("test", config);
        sensor.add(percs);
        Metric p25 = this.metrics.metrics().get("test.p25");
        Metric p50 = this.metrics.metrics().get("test.p50");
        Metric p75 = this.metrics.metrics().get("test.p75");

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
