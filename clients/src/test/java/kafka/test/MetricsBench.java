package kafka.test;

import java.util.Arrays;

import kafka.common.metrics.Metrics;
import kafka.common.metrics.Sensor;
import kafka.common.metrics.stats.Avg;
import kafka.common.metrics.stats.Count;
import kafka.common.metrics.stats.Max;
import kafka.common.metrics.stats.Percentile;
import kafka.common.metrics.stats.Percentiles;
import kafka.common.metrics.stats.Percentiles.BucketSizing;

public class MetricsBench {

    public static void main(String[] args) {
        long iters = Long.parseLong(args[0]);
        Metrics metrics = new Metrics();
        Sensor parent = metrics.sensor("parent");
        Sensor child = metrics.sensor("child", parent);
        for (Sensor sensor : Arrays.asList(parent, child)) {
            sensor.add(sensor.name() + ".avg", new Avg());
            sensor.add(sensor.name() + ".count", new Count());
            sensor.add(sensor.name() + ".max", new Max());
            sensor.add(new Percentiles(1024,
                                       0.0,
                                       iters,
                                       BucketSizing.CONSTANT,
                                       new Percentile(sensor.name() + ".median", 50.0),
                                       new Percentile(sensor.name() + ".p_99", 99.0)));
        }
        long start = System.nanoTime();
        for (int i = 0; i < iters; i++)
            child.record(i);
        double ellapsed = (System.nanoTime() - start) / (double) iters;
        System.out.println(String.format("%.2f ns per metric recording.", ellapsed));
    }
}
