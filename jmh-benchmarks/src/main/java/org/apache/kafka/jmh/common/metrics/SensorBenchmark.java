package org.apache.kafka.jmh.common.metrics;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
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
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SensorBenchmark {

    @State(Scope.Benchmark)
    public static class SensorWithNoStats {

        Metrics metrics = new Metrics();

        List<Sensor> sensors = new ArrayList<>();
        @Param({"1", "5", "20"})
        int numSensors;

        @Setup
        public void init() {
            for (int i = 0; i < numSensors; i++) {
                final String prefix = "test-sensor" + i;
                Sensor sensor = metrics.sensor(prefix);
                addStats(sensor, prefix);
                sensors.add(sensor);
            }
        }

        public void record() {
            for (Sensor s : sensors) {
                s.record();
            }
        }

        public void addStats(Sensor sensor, String prefix) {
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
    public void recordBenchmark(SensorWithNoStats state) {
        state.record();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void recordWithMaxBenchmark(SensorWithMax state) {
        state.record();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void recordWithMinMaxBenchmark(SensorWithMinMax state) {
        state.record();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void recordWithAvgBenchmark(SensorWithAvg state) {
        state.record();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void recordWithMaxAvgBenchmark(SensorWithMaxAvg state) {
        state.record();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void recordWithMinMaxAvgBenchmark(SensorWithMinMaxAvg state) {
        state.record();
    }
}
