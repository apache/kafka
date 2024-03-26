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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.TokenBucket;
import org.apache.kafka.common.metrics.stats.WindowedSum;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SensorBenchmarkTest {

    @Param({"10", "50", "100", "500", "1000", "5000", "10000"})
    private int threadCount;


    // From SensorTest
    @Benchmark
    public void testCheckQuotasInMultiThreads() throws InterruptedException, ExecutionException {
        final Metrics metrics = new Metrics(new MetricConfig().quota(Quota.upperBound(Double.MAX_VALUE))
            // decreasing the value of time window make SampledStat always record the given value
            .timeWindow(1, TimeUnit.MILLISECONDS)
            // increasing the value of samples make SampledStat store more samples
            .samples(100));
        final Sensor sensor = metrics.sensor("sensor");

        // assertTrue(sensor.add(metrics.metricName("test-metric", "test-group"), new Rate()));
        final CountDownLatch latch = new CountDownLatch(1);
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        List<Future<Throwable>> workers = new ArrayList<>(threadCount);
        boolean needShutdown = true;
        try {
            for (int i = 0; i != threadCount; ++i) {
                final int index = i;
                workers.add(service.submit(() -> {
                    try {
                        // assertTrue(latch.await(5, TimeUnit.SECONDS));
                        for (int j = 0; j != 20; ++j) {
                            sensor.record(j * index, System.currentTimeMillis() + j, false);
                            sensor.checkQuotas();
                        }
                        return null;
                    } catch (Throwable e) {
                        return e;
                    }
                }));
            }
            latch.countDown();
            service.shutdown();
            // assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
            needShutdown = false;
            for (Future<Throwable> callable : workers) {
                // assertTrue(callable.isDone(), "If this failure happen frequently, we can try to increase the wait time");
                // assertNull(callable.get(), "Sensor#checkQuotas SHOULD be thread-safe!");
            }
        } finally {
            if (needShutdown) {
                service.shutdownNow();
            }
        }
    }
}
