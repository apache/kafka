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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SensorTest {

    private static final MetricConfig INFO_CONFIG = new MetricConfig().recordLevel(Sensor.RecordingLevel.INFO);
    private static final MetricConfig DEBUG_CONFIG = new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG);
    private static final MetricConfig TRACE_CONFIG = new MetricConfig().recordLevel(Sensor.RecordingLevel.TRACE);

    @Test
    public void testRecordLevelEnum() {
        Sensor.RecordingLevel configLevel = Sensor.RecordingLevel.INFO;
        assertTrue(Sensor.RecordingLevel.INFO.shouldRecord(configLevel.id));
        assertFalse(Sensor.RecordingLevel.DEBUG.shouldRecord(configLevel.id));
        assertFalse(Sensor.RecordingLevel.TRACE.shouldRecord(configLevel.id));

        configLevel = Sensor.RecordingLevel.DEBUG;
        assertTrue(Sensor.RecordingLevel.INFO.shouldRecord(configLevel.id));
        assertTrue(Sensor.RecordingLevel.DEBUG.shouldRecord(configLevel.id));
        assertFalse(Sensor.RecordingLevel.TRACE.shouldRecord(configLevel.id));

        configLevel = Sensor.RecordingLevel.TRACE;
        assertTrue(Sensor.RecordingLevel.INFO.shouldRecord(configLevel.id));
        assertTrue(Sensor.RecordingLevel.DEBUG.shouldRecord(configLevel.id));
        assertTrue(Sensor.RecordingLevel.TRACE.shouldRecord(configLevel.id));

        assertEquals(Sensor.RecordingLevel.valueOf(Sensor.RecordingLevel.DEBUG.toString()),
            Sensor.RecordingLevel.DEBUG);
        assertEquals(Sensor.RecordingLevel.valueOf(Sensor.RecordingLevel.INFO.toString()),
            Sensor.RecordingLevel.INFO);
        assertEquals(Sensor.RecordingLevel.valueOf(Sensor.RecordingLevel.TRACE.toString()),
            Sensor.RecordingLevel.TRACE);
    }

    @Test
    public void testShouldRecordForInfoLevelSensor() {
        Sensor infoSensor = new Sensor(null, "infoSensor", null, INFO_CONFIG, new SystemTime(),
            0, Sensor.RecordingLevel.INFO);
        assertTrue(infoSensor.shouldRecord());

        infoSensor = new Sensor(null, "infoSensor", null, DEBUG_CONFIG, new SystemTime(),
            0, Sensor.RecordingLevel.INFO);
        assertTrue(infoSensor.shouldRecord());

        infoSensor = new Sensor(null, "infoSensor", null, TRACE_CONFIG, new SystemTime(),
            0, Sensor.RecordingLevel.INFO);
        assertTrue(infoSensor.shouldRecord());
    }

    @Test
    public void testShouldRecordForDebugLevelSensor() {
        Sensor debugSensor = new Sensor(null, "debugSensor", null, INFO_CONFIG, new SystemTime(),
            0, Sensor.RecordingLevel.DEBUG);
        assertFalse(debugSensor.shouldRecord());

        debugSensor = new Sensor(null, "debugSensor", null, DEBUG_CONFIG, new SystemTime(),
             0, Sensor.RecordingLevel.DEBUG);
        assertTrue(debugSensor.shouldRecord());

        debugSensor = new Sensor(null, "debugSensor", null, TRACE_CONFIG, new SystemTime(),
             0, Sensor.RecordingLevel.DEBUG);
        assertTrue(debugSensor.shouldRecord());
    }

    @Test
    public void testShouldRecordForTraceLevelSensor() {
        Sensor traceSensor = new Sensor(null, "traceSensor", null, INFO_CONFIG, new SystemTime(),
             0, Sensor.RecordingLevel.TRACE);
        assertFalse(traceSensor.shouldRecord());

        traceSensor = new Sensor(null, "traceSensor", null, DEBUG_CONFIG, new SystemTime(),
             0, Sensor.RecordingLevel.TRACE);
        assertFalse(traceSensor.shouldRecord());

        traceSensor = new Sensor(null, "traceSensor", null, TRACE_CONFIG, new SystemTime(),
             0, Sensor.RecordingLevel.TRACE);
        assertTrue(traceSensor.shouldRecord());
    }

    @Test
    public void testExpiredSensor() {
        MetricConfig config = new MetricConfig();
        Time mockTime = new MockTime();
        try (Metrics metrics = new Metrics(config, Arrays.asList(new JmxReporter()), mockTime, true)) {
            long inactiveSensorExpirationTimeSeconds = 60L;
            Sensor sensor = new Sensor(metrics, "sensor", null, config, mockTime,
                    inactiveSensorExpirationTimeSeconds, Sensor.RecordingLevel.INFO);

            assertTrue(sensor.add(metrics.metricName("test1", "grp1"), new Avg()));

            Map<String, String> emptyTags = Collections.emptyMap();
            MetricName rateMetricName = new MetricName("rate", "test", "", emptyTags);
            MetricName totalMetricName = new MetricName("total", "test", "", emptyTags);
            Meter meter = new Meter(rateMetricName, totalMetricName);
            assertTrue(sensor.add(meter));

            mockTime.sleep(TimeUnit.SECONDS.toMillis(inactiveSensorExpirationTimeSeconds + 1));
            assertFalse(sensor.add(metrics.metricName("test3", "grp1"), new Avg()));
            assertFalse(sensor.add(meter));
        }
    }

    @Test
    public void testIdempotentAdd() {
        final Metrics metrics = new Metrics();
        final Sensor sensor = metrics.sensor("sensor");

        assertTrue(sensor.add(metrics.metricName("test-metric", "test-group"), new Avg()));

        // adding the same metric to the same sensor is a no-op
        assertTrue(sensor.add(metrics.metricName("test-metric", "test-group"), new Avg()));


        // but adding the same metric to a DIFFERENT sensor is an error
        final Sensor anotherSensor = metrics.sensor("another-sensor");
        try {
            anotherSensor.add(metrics.metricName("test-metric", "test-group"), new Avg());
            fail("should have thrown");
        } catch (final IllegalArgumentException ignored) {
            // pass
        }

        // note that adding a different metric with the same name is also a no-op
        assertTrue(sensor.add(metrics.metricName("test-metric", "test-group"), new WindowedSum()));

        // so after all this, we still just have the original metric registered
        assertEquals(1, sensor.metrics().size());
        assertEquals(org.apache.kafka.common.metrics.stats.Avg.class, sensor.metrics().get(0).measurable().getClass());
    }

    /**
     * The Sensor#checkQuotas should be thread-safe since the method may be used by many ReplicaFetcherThreads.
     */
    @Test
    public void testCheckQuotasInMultiThreads() throws InterruptedException, ExecutionException {
        final Metrics metrics = new Metrics(new MetricConfig().quota(Quota.upperBound(Double.MAX_VALUE))
            // decreasing the value of time window make SampledStat always record the given value
            .timeWindow(1, TimeUnit.MILLISECONDS)
            // increasing the value of samples make SampledStat store more samples
            .samples(100));
        final Sensor sensor = metrics.sensor("sensor");

        assertTrue(sensor.add(metrics.metricName("test-metric", "test-group"), new Rate()));
        final int threadCount = 10;
        final CountDownLatch latch = new CountDownLatch(1);
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        List<Future<Throwable>> workers = new ArrayList<>(threadCount);
        boolean needShutdown = true;
        try {
            for (int i = 0; i != threadCount; ++i) {
                final int index = i;
                workers.add(service.submit(new Callable<Throwable>() {
                    @Override
                    public Throwable call() {
                        try {
                            assertTrue(latch.await(5, TimeUnit.SECONDS));
                            for (int j = 0; j != 20; ++j) {
                                sensor.record(j * index, System.currentTimeMillis() + j, false);
                                sensor.checkQuotas();
                            }
                            return null;
                        } catch (Throwable e) {
                            return e;
                        }
                    }
                }));
            }
            latch.countDown();
            service.shutdown();
            assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
            needShutdown = false;
            for (Future<Throwable> callable : workers) {
                assertTrue("If this failure happen frequently, we can try to increase the wait time", callable.isDone());
                assertNull("Sensor#checkQuotas SHOULD be thread-safe!", callable.get());
            }
        } finally {
            if (needShutdown) {
                service.shutdownNow();
            }
        }
    }

    @Test
    public void shouldReturnPresenceOfMetrics() {
        final Metrics metrics = new Metrics();
        final Sensor sensor = metrics.sensor("sensor");

        assertThat(sensor.hasMetrics(), is(false));

        sensor.add(
            new MetricName("name1", "group1", "description1", Collections.emptyMap()),
            new WindowedSum()
        );

        assertThat(sensor.hasMetrics(), is(true));

        sensor.add(
            new MetricName("name2", "group2", "description2", Collections.emptyMap()),
            new CumulativeCount()
        );

        assertThat(sensor.hasMetrics(), is(true));
    }

    @Test
    public void testStrictQuotaEnforcementWithRate() {
        final Time time = new MockTime(0, System.currentTimeMillis(), 0);
        final Metrics metrics = new Metrics(time);
        final Sensor sensor = metrics.sensor("sensor", new MetricConfig()
            .quota(Quota.upperBound(2))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(11));
        final MetricName metricName = metrics.metricName("rate", "test-group");
        assertTrue(sensor.add(metricName, new Rate()));
        final KafkaMetric rateMetric = metrics.metric(metricName);

        // Recording a first value at T+0 to bring the avg rate to 3 which is already
        // above the quota.
        strictRecord(sensor, 30, time.milliseconds());
        assertEquals(3, rateMetric.measurableValue(time.milliseconds()), 0.1);

        // Theoretically, we should wait 5s to bring back the avg rate to the define quota:
        // ((30 / 10) - 2) / 2 * 10 = 5s
        time.sleep(5000);

        // But, recording a second value is rejected because the avg rate is still equal
        // to 3 after 5s.
        assertEquals(3, rateMetric.measurableValue(time.milliseconds()), 0.1);
        assertThrows(QuotaViolationException.class, () -> strictRecord(sensor, 30, time.milliseconds()));

        metrics.close();
    }

    @Test
    public void testStrictQuotaEnforcementWithTokenBucket() {
        final Time time = new MockTime(0, System.currentTimeMillis(), 0);
        final Metrics metrics = new Metrics(time);
        final Sensor sensor = metrics.sensor("sensor", new MetricConfig()
            .quota(Quota.upperBound(2))
            .timeWindow(1, TimeUnit.SECONDS)
            .samples(10));
        final MetricName metricName = metrics.metricName("credits", "test-group");
        assertTrue(sensor.add(metricName, new TokenBucket()));
        final KafkaMetric tkMetric = metrics.metric(metricName);

        // Recording a first value at T+0 to bring the remaining credits below zero
        strictRecord(sensor, 30, time.milliseconds());
        assertEquals(-10, tkMetric.measurableValue(time.milliseconds()), 0.1);

        // Theoretically, we should wait 5s to bring back the avg rate to the define quota:
        // 10 / 2 = 5s
        time.sleep(5000);

        // Unlike the default rate based on a windowed sum, it works as expected.
        assertEquals(0, tkMetric.measurableValue(time.milliseconds()), 0.1);
        strictRecord(sensor, 30, time.milliseconds());
        assertEquals(-30, tkMetric.measurableValue(time.milliseconds()), 0.1);

        metrics.close();
    }

    private void strictRecord(Sensor sensor, double value, long timeMs) {
        synchronized (sensor) {
            sensor.checkQuotas(timeMs);
            sensor.record(value, timeMs, false);
        }
    }

    @Test
    public void testRecordAndCheckQuotaUseMetricConfigOfEachStat() {
        final Time time = new MockTime(0, System.currentTimeMillis(), 0);
        final Metrics metrics = new Metrics(time);
        final Sensor sensor = metrics.sensor("sensor");

        final MeasurableStat stat1 = Mockito.mock(MeasurableStat.class);
        final MetricName stat1Name = metrics.metricName("stat1", "test-group");
        final MetricConfig stat1Config = new MetricConfig().quota(Quota.upperBound(5));
        sensor.add(stat1Name, stat1, stat1Config);

        final MeasurableStat stat2 = Mockito.mock(MeasurableStat.class);
        final MetricName stat2Name = metrics.metricName("stat2", "test-group");
        final MetricConfig stat2Config = new MetricConfig().quota(Quota.upperBound(10));
        sensor.add(stat2Name, stat2, stat2Config);

        sensor.record(10, 1);
        Mockito.verify(stat1).record(stat1Config, 10, 1);
        Mockito.verify(stat2).record(stat2Config, 10, 1);

        sensor.checkQuotas(2);
        Mockito.verify(stat1).measure(stat1Config, 2);
        Mockito.verify(stat2).measure(stat2Config, 2);

        metrics.close();
    }

    @Test
    public void testUpdatingMetricConfigIsReflectedInTheSensor() {
        final Time time = new MockTime(0, System.currentTimeMillis(), 0);
        final Metrics metrics = new Metrics(time);
        final Sensor sensor = metrics.sensor("sensor");

        final MeasurableStat stat = Mockito.mock(MeasurableStat.class);
        final MetricName statName = metrics.metricName("stat", "test-group");
        final MetricConfig statConfig = new MetricConfig().quota(Quota.upperBound(5));
        sensor.add(statName, stat, statConfig);

        sensor.record(10, 1);
        Mockito.verify(stat).record(statConfig, 10, 1);

        sensor.checkQuotas(2);
        Mockito.verify(stat).measure(statConfig, 2);

        // Update the config of the KafkaMetric
        final MetricConfig newConfig = new MetricConfig().quota(Quota.upperBound(10));
        metrics.metric(statName).config(newConfig);

        sensor.record(10, 3);
        Mockito.verify(stat).record(newConfig, 10, 3);

        sensor.checkQuotas(4);
        Mockito.verify(stat).measure(newConfig, 4);

        metrics.close();
    }
}