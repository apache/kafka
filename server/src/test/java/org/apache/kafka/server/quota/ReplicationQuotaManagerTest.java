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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.config.ReplicationQuotaManagerConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicationQuotaManagerTest {

    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(new MetricConfig(), List.of(), time);

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void shouldThrottleOnlyDefinedReplicas() {
        ReplicationQuotaManager quota = new ReplicationQuotaManager(new ReplicationQuotaManagerConfig(), metrics, QuotaType.FETCH, time);
        quota.markThrottled("topic1", List.of(1, 2, 3));

        assertTrue(quota.isThrottled(new TopicPartition("topic1", 1)));
        assertTrue(quota.isThrottled(new TopicPartition("topic1", 2)));
        assertTrue(quota.isThrottled(new TopicPartition("topic1", 3)));
        assertFalse(quota.isThrottled(new TopicPartition("topic1", 4)));
    }

    @Test
    public void shouldExceedQuotaThenReturnBackBelowBoundAsTimePasses() {
        ReplicationQuotaManager quota = new ReplicationQuotaManager(new ReplicationQuotaManagerConfig(10, 1), metrics, QuotaType.LEADER_REPLICATION, time);

        //Given
        quota.updateQuota(new Quota(100, true));

        //Quota should not be broken when we start
        assertFalse(quota.isQuotaExceeded());

        //First window is fixed, so we'll skip it
        time.sleep(1000);

        //When we record up to the quota value after half a window
        time.sleep(500);
        quota.record(1);

        //Then it should not break the quota
        assertFalse(quota.isQuotaExceeded());

        //When we record half the quota (halfway through the window), we still should not break
        quota.record(149); //150B, 1.5s
        assertFalse(quota.isQuotaExceeded());

        //Add a byte to push over quota
        quota.record(1); //151B, 1.5s

        //Then it should break the quota
        assertEquals(151 / 1.5, rate(metrics), 0); //151B, 1.5s
        assertTrue(quota.isQuotaExceeded());

        //When we sleep for the remaining half the window
        time.sleep(500); //151B, 2s

        //Then Our rate should have halved (i.e. back down below the quota)
        assertFalse(quota.isQuotaExceeded());
        assertEquals(151d / 2, rate(metrics), 0.1); //151B, 2s

        //When we sleep for another half a window (now halfway through second window)
        time.sleep(500);
        quota.record(99); //250B, 2.5s

        //Then the rate should be exceeded again
        assertEquals(250 / 2.5, rate(metrics), 0); //250B, 2.5s
        assertFalse(quota.isQuotaExceeded());
        quota.record(1);
        assertTrue(quota.isQuotaExceeded());
        assertEquals(251 / 2.5, rate(metrics), 0);

        //Sleep for 2 more window
        time.sleep(2 * 1000); //so now at 3.5s
        assertFalse(quota.isQuotaExceeded());
        assertEquals(251 / 4.5, rate(metrics), 0);
    }

    private double rate(Metrics metrics) {
        MetricName metricName = metrics.metricName("byte-rate", QuotaType.LEADER_REPLICATION.toString(), "Tracking byte-rate for " + QuotaType.LEADER_REPLICATION);
        return (double) metrics.metrics().get(metricName).metricValue();
    }

    @Test
    public void shouldSupportWildcardThrottledReplicas() {
        ReplicationQuotaManager quota = new ReplicationQuotaManager(new ReplicationQuotaManagerConfig(), metrics, QuotaType.LEADER_REPLICATION, time);

        //When
        quota.markThrottled("MyTopic");

        //Then
        assertTrue(quota.isThrottled(new TopicPartition("MyTopic", 0)));
        assertFalse(quota.isThrottled(new TopicPartition("MyOtherTopic", 0)));
    }

}
