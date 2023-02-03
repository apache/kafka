package org.apache.kafka.jmh.common.metrics;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.SimpleRate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedSum;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark the impact of metric sensors to assess the inclusion of additional metrics and compare
 * different stats.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SensorBenchmark {

    @State(Scope.Benchmark)
    public static class SensorWithNoStats {

        Metrics metrics = new Metrics();

        List<Sensor> sensors = new ArrayList<>();
        Random random = new Random();
        // Estimated number of sensors included in different transactions
        // (e.g. Connector Sink Task iteration has ~5 at the moment)
        @Param({"1", "5", "20"})
        int numSensors;

        @Setup
        public void init() {
            for (int i = 0; i < numSensors; i++) {
                final String prefix = "test-sensor" + i;
                Sensor parent = metrics.sensor("parent");
                Sensor child = metrics.sensor("child", parent);
                addStats(child, prefix);
                sensors.add(child);
            }
        }

        public double record(double value) {
            for (Sensor s : sensors) {
                s.record(value);
            }
            return value;
        }

        public double value() {
            return random.nextDouble() * 100.0;
        }

        public void addStats(Sensor sensor, String prefix) {
        }

    }

    @State(Scope.Benchmark)
    public static class SensorWithValue extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(metrics.metricName(prefix + "-value", "benchmark"), new Value());
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithMax extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(metrics.metricName(prefix + "-max", "benchmark"), new Max());
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithWindowedSum extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(metrics.metricName(prefix + "-total", "benchmark"), new WindowedSum());
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithRate extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(metrics.metricName(prefix + "-rate", "benchmark"), new Rate());
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithSimpleRate extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(metrics.metricName(prefix + "-rate", "benchmark"), new SimpleRate());
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithMeter extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(new Meter(
                    metrics.metricName(prefix + "-rate", "benchmark"),
                    metrics.metricName(prefix + "-total", "benchmark")
            ));
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithPercentiles extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(new Percentiles(1024,
                    0.0,
                    100.0,
                    BucketSizing.CONSTANT,
                    new Percentile(metrics.metricName(sensor.name() + ".median", "grp1"), 50.0),
                    new Percentile(metrics.metricName(sensor.name() + ".p_99", "grp1"), 99.0)));
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithMinMax extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(metrics.metricName(prefix + "-min", "benchmark"), new Min());
            sensor.add(metrics.metricName(prefix + "-max", "benchmark"), new Max());
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithAvg extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(metrics.metricName(prefix + "-avg", "benchmark"), new Avg());
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithMaxAvg extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(metrics.metricName(prefix + "-max", "benchmark"), new Max());
            sensor.add(metrics.metricName(prefix + "-avg", "benchmark"), new Avg());
        }

    }

    @State(Scope.Benchmark)
    public static class SensorWithMinMaxAvg extends SensorWithNoStats {

        @Override
        public void addStats(Sensor sensor, String prefix) {
            sensor.add(metrics.metricName(prefix + "-min", "benchmark"), new Min());
            sensor.add(metrics.metricName(prefix + "-max", "benchmark"), new Max());
            sensor.add(metrics.metricName(prefix + "-avg", "benchmark"), new Avg());
        }
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordBenchmark(SensorWithNoStats state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithValueBenchmark(SensorWithValue state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithMaxBenchmark(SensorWithMax state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithAvgBenchmark(SensorWithAvg state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithWindowedSumBenchmark(SensorWithWindowedSum state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithPercentileBenchmark(SensorWithPercentiles state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithRateBenchmark(SensorWithRate state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithSimpleRateBenchmark(SensorWithSimpleRate state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithMeterBenchmark(SensorWithMeter state) {
        return state.record(state.value());
    }


    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithMinMaxBenchmark(SensorWithMinMax state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithMaxAvgBenchmark(SensorWithMaxAvg state) {
        return state.record(state.value());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double recordWithMinMaxAvgBenchmark(SensorWithMinMaxAvg state) {
        return state.record(state.value());
    }
}
