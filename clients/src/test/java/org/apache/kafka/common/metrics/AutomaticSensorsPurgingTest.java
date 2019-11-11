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

import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AutomaticSensorsPurgingTest {
    private static final long EXPIRATION_TIME = 4L;

    private MockTime time = new MockTime();
    private Metrics metrics;

    @Before
    public void setup() {
        metrics = new Metrics(new MetricConfig(), singletonList(new JmxReporter()), time, true);
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    private void waitingLessThanExpirationTime() {
        time.sleep(TimeUnit.MILLISECONDS.convert(
                EXPIRATION_TIME - EXPIRATION_TIME / 2,
                TimeUnit.SECONDS
        ));
    }

    private void waitingLongerThanExpirationTime() {
        time.sleep(TimeUnit.MILLISECONDS.convert(
                EXPIRATION_TIME + EXPIRATION_TIME / 2,
                TimeUnit.SECONDS
        ));
    }

    private void assertThatSensorAndMetricsAreNotPurged() {
        assertNotNull(
                "Sensor test.sensor should have been purged",
                metrics.getSensor("test.sensor")
        );
        assertNotNull(
                "MetricName test.sensor.count should have been purged",
                metrics.metric(metrics.metricName("test.sensor.count", "group"))
        );
    }

    private void assertThatSensorAndMetricsArePurged() {
        assertNull("Sensor test.sensor should have been purged", metrics.getSensor("test.sensor"));
        assertNull(
                "MetricName test.sensor.count should have been purged",
                metrics.metric(metrics.metricName("test.sensor.count", "group"))
        );
    }

    @Test
    public void sensorIsRemoved_afterBeingInactive_forExpirationTime() {
        metrics.sensor("test.sensor", null, EXPIRATION_TIME)
                .add(metrics.metricName("test.sensor.count", "group"), new WindowedCount());

        Metrics.ExpireSensorTask purger = metrics.new ExpireSensorTask();

        waitingLessThanExpirationTime();
        purger.run();

        assertThatSensorAndMetricsAreNotPurged();

        waitingLongerThanExpirationTime();
        purger.run();

        assertThatSensorAndMetricsArePurged();
    }

    @Test
    public void sensorIsNotRemoved_whenExpirationTimeWasNotExceeded() {
        long expirationTime = 4L;
        metrics.sensor("test.sensor", null, expirationTime)
                .add(metrics.metricName("test.sensor.count", "group"), new WindowedCount());

        Metrics.ExpireSensorTask purger = metrics.new ExpireSensorTask();

        waitingLessThanExpirationTime();
        purger.run();

        assertThatSensorAndMetricsAreNotPurged();
    }

    @Test
    public void sensorCanBeRecreatedAfterPurging() {
        long expirationTime = 4L;
        metrics.sensor("test.sensor", null, expirationTime)
                .add(metrics.metricName("test.sensor.count", "group"), new WindowedCount());

        Metrics.ExpireSensorTask purger = metrics.new ExpireSensorTask();

        waitingLongerThanExpirationTime();
        purger.run();

        metrics.sensor("test.sensor", null, expirationTime)
                .add(metrics.metricName("test.sensor.count", "group"), new WindowedCount());

        assertThatSensorAndMetricsAreNotPurged();
    }
}
