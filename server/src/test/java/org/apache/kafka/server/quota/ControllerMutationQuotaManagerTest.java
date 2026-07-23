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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.TokenBucket;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.kafka.server.quota.ControllerMutationQuotaManager.throttleTimeMs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ControllerMutationQuotaManagerTest extends BaseClientQuotaManagerTest {

    private static final String USER = "ANONYMOUS";
    private static final String CLIENT_ID = "test-client";

    private final ClientQuotaManagerConfig config = new ClientQuotaManagerConfig(10, 1);

    @Test
    public void testStrictControllerMutationQuotaViolation() {
        MockTime time = new MockTime(0, System.currentTimeMillis(), 0);
        try (Metrics metrics = new Metrics(time)) {
            Sensor sensor = metrics.sensor("sensor", new MetricConfig()
                    .quota(Quota.upperBound(10))
                    .timeWindow(1, TimeUnit.SECONDS)
                    .samples(10));
            MetricName metricName = metrics.metricName("rate", "test-group");
            assertTrue(sensor.add(metricName, new TokenBucket()));

            StrictControllerMutationQuota quota = new StrictControllerMutationQuota(time, sensor);
            assertFalse(quota.isExceeded());

            // Recording a first value at T to bring the tokens to 10. Value is accepted
            // because the quota is not exhausted yet.
            quota.record(90);
            assertFalse(quota.isExceeded());
            assertEquals(0, quota.throttleTime());

            // Recording a second value at T to bring the tokens to -80. Value is accepted
            quota.record(90);
            assertFalse(quota.isExceeded());
            assertEquals(0, quota.throttleTime());

            // Recording a third value at T is rejected immediately because there are no
            // tokens available in the bucket.
            assertThrows(ThrottlingQuotaExceededException.class, () -> quota.record(90));
            assertTrue(quota.isExceeded());
            assertEquals(8000, quota.throttleTime());

            // Throttle time is adjusted with time
            time.sleep(5000);
            assertEquals(3000, quota.throttleTime());
        }
    }

    @Test
    public void testPermissiveControllerMutationQuotaViolation() {
        MockTime time = new MockTime(0, System.currentTimeMillis(), 0);
        try (Metrics metrics = new Metrics(time)) {
            Sensor sensor = metrics.sensor("sensor", new MetricConfig()
                    .quota(Quota.upperBound(10))
                    .timeWindow(1, TimeUnit.SECONDS)
                    .samples(10));
            MetricName metricName = metrics.metricName("rate", "test-group");
            assertTrue(sensor.add(metricName, new TokenBucket()));

            PermissiveControllerMutationQuota quota = new PermissiveControllerMutationQuota(time, sensor);
            assertFalse(quota.isExceeded());

            // Recording a first value at T to bring the tokens 10. Value is accepted
            // because the quota is not exhausted yet.
            quota.record(90);
            assertFalse(quota.isExceeded());
            assertEquals(0, quota.throttleTime());

            // Recording a second value at T to bring the tokens to -80. Value is accepted
            quota.record(90);
            assertFalse(quota.isExceeded());
            assertEquals(8000, quota.throttleTime());

            // Recording a third value at T to bring the tokens to -170. Value is accepted
            // even though the quota is exhausted.
            quota.record(90);
            assertFalse(quota.isExceeded()); // quota is never exceeded
            assertEquals(17000, quota.throttleTime());

            // Throttle time is adjusted with time
            time.sleep(5000);
            assertEquals(12000, quota.throttleTime());
        }
    }

    private void withQuotaManager(Consumer<ControllerMutationQuotaManager> f) {
        ControllerMutationQuotaManager quotaManager = new ControllerMutationQuotaManager(config, metrics, time, "", Optional.empty());
        try {
            f.accept(quotaManager);
        } finally {
            quotaManager.shutdown();
        }
    }

    @Test
    public void testThrottleTime() {
        MockTime time = new MockTime(0, System.currentTimeMillis(), 0);
        try (Metrics metrics = new Metrics(time)) {
            Sensor sensor = metrics.sensor("sensor");
            MetricName metricName = metrics.metricName("tokens", "test-group");
            sensor.add(metricName, new TokenBucket());
            KafkaMetric metric = metrics.metric(metricName);

            assertEquals(0, throttleTimeMs(new QuotaViolationException(metric, 0, 10)));
            assertEquals(500, throttleTimeMs(new QuotaViolationException(metric, -5, 10)));
            assertEquals(1000, throttleTimeMs(new QuotaViolationException(metric, -10, 10)));
        }
    }

    @Test
    public void testControllerMutationQuotaViolation() {
        withQuotaManager(quotaManager -> {
            quotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity(USER)),
                    Optional.of(new ClientQuotaManager.ClientIdEntity(CLIENT_ID)),
                    Optional.of(Quota.upperBound(10))
            );
            KafkaMetric queueSizeMetric = metrics.metrics().get(
                metrics.metricName("queue-size", QuotaType.CONTROLLER_MUTATION.toString(), ""));

            // Verify that there is no quota violation if we remain under the quota.
            for (int i = 0; i < 10; i++) {
                assertEquals(0, maybeRecord(quotaManager, USER, CLIENT_ID, 10));
                time.sleep(1000);
            }
            assertEquals(0, ((Double) queueSizeMetric.metricValue()).intValue());

            // Create a spike worth of 110 mutations.
            // Current tokens in the bucket = 100
            // As we use the Strict enforcement, the quota is checked before updating the rate. Hence,
            // the spike is accepted and no quota violation error is raised.
            int throttleTime = maybeRecord(quotaManager, USER, CLIENT_ID, 110);
            assertEquals(0, throttleTime, "Should not be throttled");

            // Create a spike worth of 110 mutations.
            // Current tokens in the bucket = 100 - 110 = -10
            // As the quota is already violated, the spike is rejected immediately without updating the
            // rate. The client must wait:
            // 10 / 10 = 1s
            throttleTime = maybeRecord(quotaManager, USER, CLIENT_ID, 110);
            assertEquals(1000, throttleTime, "Should be throttled");

            // Throttle
            throttle(quotaManager, throttleTime, callback);
            assertEquals(1, ((Double) queueSizeMetric.metricValue()).intValue());

            // After a request is delayed, the callback cannot be triggered immediately
            quotaManager.processThrottledChannelReaperDoWork();
            assertEquals(0, numCallbacks);

            // Callback can only be triggered after the delay time passes
            time.sleep(throttleTime);
            quotaManager.processThrottledChannelReaperDoWork();
            assertEquals(0, ((Double) queueSizeMetric.metricValue()).intValue());
            assertEquals(1, numCallbacks);

            // Retry to spike worth of 110 mutations after having waited the required throttle time.
            // Current tokens in the bucket = 0
            throttleTime = maybeRecord(quotaManager, USER, CLIENT_ID, 110);
            assertEquals(0, throttleTime, "Should not be throttled");
        });
    }

    @Test
    public void testNewStrictQuotaForReturnsUnboundedQuotaWhenQuotaIsDisabled() {
        withQuotaManager(quotaManager ->
                assertEquals(ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA,
                        quotaManager.newStrictQuotaFor(buildSession(USER), CLIENT_ID)));
    }

    @Test
    public void testNewStrictQuotaForReturnsStrictQuotaWhenQuotaIsEnabled() {
        withQuotaManager(quotaManager -> {
            quotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity(USER)),
                    Optional.of(new ClientQuotaManager.ClientIdEntity(CLIENT_ID)),
                    Optional.of(Quota.upperBound(10))
            );
            ControllerMutationQuota quota = quotaManager.newStrictQuotaFor(buildSession(USER), CLIENT_ID);
            assertInstanceOf(StrictControllerMutationQuota.class, quota);
        });
    }

    @Test
    public void testNewPermissiveQuotaForReturnsUnboundedQuotaWhenQuotaIsDisabled() {
        withQuotaManager(quotaManager ->
                assertEquals(ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA,
                        quotaManager.newPermissiveQuotaFor(buildSession(USER), CLIENT_ID)));
    }

    @Test
    public void testNewPermissiveQuotaForReturnsStrictQuotaWhenQuotaIsEnabled() {
        withQuotaManager(quotaManager -> {
            quotaManager.updateQuota(
                    Optional.of(new ClientQuotaManager.UserEntity(USER)),
                    Optional.of(new ClientQuotaManager.ClientIdEntity(CLIENT_ID)),
                    Optional.of(Quota.upperBound(10))
            );
            ControllerMutationQuota quota = quotaManager.newPermissiveQuotaFor(buildSession(USER), CLIENT_ID);
            assertInstanceOf(PermissiveControllerMutationQuota.class, quota);
        });
    }

}
