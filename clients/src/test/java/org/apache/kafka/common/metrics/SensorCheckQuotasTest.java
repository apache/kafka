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

import org.apache.kafka.common.metrics.stats.Rate;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SensorCheckQuotasTest {

    private static final int THREAD_COUNT = 10;
    private static final Throwable QUOTAS_CHECK_WITHOUT_EXCEPTION = null;

    private Sensor sensor;
    private ExecutorService service;
    private List<Future<Throwable>> workers;

    @Before
    public void setUp() {
        Metrics metrics = new Metrics(
                new MetricConfig().quota(Quota.upperBound(Double.MAX_VALUE))
                        // decreasing the value of time window make SampledStat always record the given value
                        .timeWindow(1, TimeUnit.MILLISECONDS)
                        // increasing the value of samples make SampledStat store more samples
                        .samples(100));
        sensor = metrics.sensor("sensor");
        sensor.add(metrics.metricName("test-metric", "test-group"), new Rate());
        service = Executors.newFixedThreadPool(THREAD_COUNT);
        workers = new ArrayList<>(THREAD_COUNT);
    }

    /**
     * The Sensor#checkQuotas should be thread-safe since the method may be used by many ReplicaFetcherThreads.
     */
    @Test
    public void testCheckQuotasInMultiThreads() throws InterruptedException, ExecutionException {
        final CountDownLatch latch = new CountDownLatch(1);
        boolean needShutdown = true;
        try {
            for (int i = 0; i != THREAD_COUNT; ++i) {
                final int index = i;
                workers.add(service.submit(() -> {
                    try {
                        assertTrue(latch.await(5, TimeUnit.SECONDS));
                        for (int j = 0; j != 20; ++j) {
                            sensor.record(j * index, System.currentTimeMillis() + j, false);
                            sensor.checkQuotas();
                        }
                        return QUOTAS_CHECK_WITHOUT_EXCEPTION;
                    } catch (Throwable e) {
                        return e;
                    }
                }));
            }
            latch.countDown();
            service.shutdown();
            assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
            needShutdown = false;
            for (Future<Throwable> callable : workers) {
                assertTrue("If this failure happen frequently, we can try to increase the wait time", callable.isDone());
                assertEquals("Sensor#checkQuotas SHOULD be thread-safe!", QUOTAS_CHECK_WITHOUT_EXCEPTION, callable.get());
            }
        } finally {
            if (needShutdown) {
                service.shutdownNow();
            }
        }
    }
}
