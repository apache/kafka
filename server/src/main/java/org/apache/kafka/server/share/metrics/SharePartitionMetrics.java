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
package org.apache.kafka.server.share.metrics;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * SharePartitionMetrics is used to track the broker-side metrics for the SharePartition.
 */
public class SharePartitionMetrics implements AutoCloseable {

    public static final String IN_FLIGHT_MESSAGE_COUNT = "InFlightMessageCount";
    public static final String IN_FLIGHT_BATCH_COUNT = "InFlightBatchCount";

    private static final String ACQUISITION_LOCK_TIMEOUT_PER_SEC = "AcquisitionLockTimeoutPerSec";
    private static final String IN_FLIGHT_BATCH_MESSAGE_COUNT = "InFlightBatchMessageCount";
    private static final String FETCH_LOCK_TIME_MS = "FetchLockTimeMs";
    private static final String FETCH_LOCK_RATIO = "FetchLockRatio";

    /**
     * Metric for the rate of acquisition lock timeouts for records.
     */
    private final Meter acquisitionLockTimeoutPerSec;
    /**
     * Metric for the number of in-flight messages for the batch.
     */
    private final Histogram inFlightBatchMessageCount;
    /**
     * Metric for the time the fetch lock is held.
     */
    private final Histogram fetchLockTimeMs;
    /**
     * Metric for the ratio of fetch lock time to the total time.
     */
    private final Histogram fetchLockRatio;

    private final Map<String, String> tags;
    private final KafkaMetricsGroup metricsGroup;

    public SharePartitionMetrics(String groupId, String topic, int partition) {
        this.tags = Utils.mkMap(
            Utils.mkEntry("group", Objects.requireNonNull(groupId)),
            Utils.mkEntry("topic", Objects.requireNonNull(topic)),
            Utils.mkEntry("partition", String.valueOf(partition))
        );
        this.metricsGroup = new KafkaMetricsGroup("kafka.server", "SharePartitionMetrics");

        this.acquisitionLockTimeoutPerSec = metricsGroup.newMeter(
            ACQUISITION_LOCK_TIMEOUT_PER_SEC,
            "acquisition lock timeout",
            TimeUnit.SECONDS,
            this.tags);

        this.inFlightBatchMessageCount = metricsGroup.newHistogram(
            IN_FLIGHT_BATCH_MESSAGE_COUNT,
            true,
            this.tags);

        this.fetchLockTimeMs = metricsGroup.newHistogram(
            FETCH_LOCK_TIME_MS,
            true,
            this.tags);

        this.fetchLockRatio = metricsGroup.newHistogram(
            FETCH_LOCK_RATIO,
            true,
            this.tags);
    }

    /**
     * Register a gauge for the in-flight message count.
     *
     * @param messageCountSupplier The supplier for the in-flight message count.
     */
    public void registerInFlightMessageCount(Supplier<Integer> messageCountSupplier) {
        metricsGroup.newGauge(
            IN_FLIGHT_MESSAGE_COUNT,
            messageCountSupplier,
            this.tags
        );
    }

    /**
     * Register a gauge for the in-flight batch count.
     *
     * @param batchCountSupplier The supplier for the in-flight batch count.
     */
    public void registerInFlightBatchCount(Supplier<Integer> batchCountSupplier) {
        metricsGroup.newGauge(
            IN_FLIGHT_BATCH_COUNT,
            batchCountSupplier,
            this.tags
        );
    }

    public void recordAcquisitionLockTimeoutPerSec(long count) {
        acquisitionLockTimeoutPerSec.mark(count);
    }

    public void recordInFlightBatchMessageCount(long count) {
        inFlightBatchMessageCount.update(count);
    }

    public void recordFetchLockTimeMs(long timeMs) {
        fetchLockTimeMs.update(timeMs);
    }

    public void recordFetchLockRatio(int value) {
        fetchLockRatio.update(value);
    }

    // Visible for testing
    public Meter acquisitionLockTimeoutPerSec() {
        return acquisitionLockTimeoutPerSec;
    }

    // Visible for testing
    public Histogram inFlightBatchMessageCount() {
        return inFlightBatchMessageCount;
    }

    // Visible for testing
    public Histogram fetchLockTimeMs() {
        return fetchLockTimeMs;
    }

    // Visible for testing
    public Histogram fetchLockRatio() {
        return fetchLockRatio;
    }

    @Override
    public void close() throws Exception {
        List.of(ACQUISITION_LOCK_TIMEOUT_PER_SEC,
            IN_FLIGHT_MESSAGE_COUNT,
            IN_FLIGHT_BATCH_COUNT,
            IN_FLIGHT_BATCH_MESSAGE_COUNT,
            FETCH_LOCK_TIME_MS,
            FETCH_LOCK_RATIO
        ).forEach(m -> metricsGroup.removeMetric(m, tags));
    }
}
