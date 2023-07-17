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
import org.openjdk.jmh.annotations.Level;
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
@BenchmarkMode({Mode.SampleTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SensorBenchmark {

    @State(Scope.Thread)
    public static class ValueProvider {
        Random random = new Random();

        double value;

        @Setup(Level.Invocation)
        public void init() {
            value = random.nextDouble() * 100.0;
        }

        public double value() {
            return value;
        }
    }

    @State(Scope.Benchmark)
    public static class SensorWithNoStats {

        Metrics metrics = new Metrics();

        List<Sensor> sensors = new ArrayList<>();

        // Estimated number of sensors included in different transactions
        // (e.g. Connector Sink Task iteration has ~5 at the moment)
        @Param({"1", "5", "10", "20"})
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
    public double recordBenchmark(SensorWithNoStats state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithValueBenchmark(SensorWithValue state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithMaxBenchmark(SensorWithMax state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithAvgBenchmark(SensorWithAvg state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithWindowedSumBenchmark(SensorWithWindowedSum state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithPercentileBenchmark(SensorWithPercentiles state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithRateBenchmark(SensorWithRate state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithSimpleRateBenchmark(SensorWithSimpleRate state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithMeterBenchmark(SensorWithMeter state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }


    @Benchmark
    public double recordWithMinMaxBenchmark(SensorWithMinMax state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithMaxAvgBenchmark(SensorWithMaxAvg state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }

    @Benchmark
    public double recordWithMinMaxAvgBenchmark(SensorWithMinMaxAvg state, ValueProvider valueProvider) {
        return state.record(valueProvider.value());
    }
}
