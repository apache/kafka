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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.image.MetadataProvenance;

import com.yammer.metrics.core.Histogram;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class BrokerServerMetrics implements AutoCloseable {
    private static final String METRIC_GROUP_NAME = "broker-metadata-metrics";

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "BrokerMetadataListener");
    private final com.yammer.metrics.core.MetricName batchProcessingTimeHistName =
            metricsGroup.metricName("MetadataBatchProcessingTimeUs", Collections.emptyMap());

    /**
     * A histogram tracking the time in microseconds it took to process batches of events.
     */
    private final Histogram batchProcessingTimeHist = KafkaYammerMetrics.defaultRegistry()
            .newHistogram(batchProcessingTimeHistName, true);

    private final com.yammer.metrics.core.MetricName batchSizeHistName =
            metricsGroup.metricName("MetadataBatchSizes", Collections.emptyMap());

    /**
     * A histogram tracking the sizes of batches that we have processed.
     */
    private final Histogram batchSizeHist = KafkaYammerMetrics.defaultRegistry()
            .newHistogram(batchSizeHistName, true);

    private final AtomicReference<MetadataProvenance> lastAppliedImageProvenance = new AtomicReference<>(MetadataProvenance.EMPTY);
    private final AtomicLong metadataLoadErrorCount = new AtomicLong(0);
    private final AtomicLong metadataApplyErrorCount = new AtomicLong(0);

    private final Metrics metrics;
    private final MetricName lastAppliedRecordOffsetName;
    private final MetricName lastAppliedRecordTimestampName;
    private final MetricName lastAppliedRecordLagMsName;
    private final MetricName metadataLoadErrorCountName;
    private final MetricName metadataApplyErrorCountName;

    public BrokerServerMetrics(Metrics metrics) {
        this.metrics = metrics;
        lastAppliedRecordOffsetName = metrics.metricName(
                "last-applied-record-offset",
                METRIC_GROUP_NAME,
                "The offset of the last record from the cluster metadata partition that was applied by the broker"
        );
        lastAppliedRecordTimestampName = metrics.metricName(
                "last-applied-record-timestamp",
                METRIC_GROUP_NAME,
                "The timestamp of the last record from the cluster metadata partition that was applied by the broker"
        );
        lastAppliedRecordLagMsName = metrics.metricName(
                "last-applied-record-lag-ms",
                METRIC_GROUP_NAME,
                "The difference between now and the timestamp of the last record from the cluster metadata partition that was applied by the broker"
        );
        metadataLoadErrorCountName = metrics.metricName(
                "metadata-load-error-count",
                METRIC_GROUP_NAME,
                "The number of errors encountered by the BrokerMetadataListener while loading the metadata log and generating a new MetadataDelta based on it."
        );
        metadataApplyErrorCountName = metrics.metricName(
                "metadata-apply-error-count",
                METRIC_GROUP_NAME,
                "The number of errors encountered by the BrokerMetadataPublisher while applying a new MetadataImage based on the latest MetadataDelta."
        );

        metrics.addMetric(lastAppliedRecordOffsetName, (config, now) -> lastAppliedImageProvenance.get().lastContainedOffset());
        metrics.addMetric(lastAppliedRecordTimestampName, (config, now) -> lastAppliedImageProvenance.get().lastContainedLogTimeMs());
        metrics.addMetric(lastAppliedRecordLagMsName, (config, now) -> now - lastAppliedImageProvenance.get().lastContainedLogTimeMs());
        metrics.addMetric(metadataLoadErrorCountName, (config, now) -> metadataLoadErrorCount.get());
        metrics.addMetric(metadataApplyErrorCountName, (config, now) -> metadataApplyErrorCount.get());
    }

    @Override
    public void close() throws Exception {
        KafkaYammerMetrics.defaultRegistry().removeMetric(batchProcessingTimeHistName);
        KafkaYammerMetrics.defaultRegistry().removeMetric(batchSizeHistName);
        Stream.of(
                lastAppliedRecordOffsetName,
                lastAppliedRecordTimestampName,
                lastAppliedRecordLagMsName,
                metadataLoadErrorCountName,
                metadataApplyErrorCountName
        ).forEach(metrics::removeMetric);
    }

    public MetricName lastAppliedRecordOffsetName() {
        return lastAppliedRecordOffsetName;
    }

    public MetricName lastAppliedRecordTimestampName() {
        return lastAppliedRecordTimestampName;
    }

    public MetricName lastAppliedRecordLagMsName() {
        return lastAppliedRecordLagMsName;
    }

    public MetricName metadataLoadErrorCountName() {
        return metadataLoadErrorCountName;
    }

    public MetricName metadataApplyErrorCountName() {
        return metadataApplyErrorCountName;
    }

    public AtomicReference<MetadataProvenance> lastAppliedImageProvenance() {
        return lastAppliedImageProvenance;
    }

    public AtomicLong metadataLoadErrorCount() {
        return metadataLoadErrorCount;
    }

    public AtomicLong metadataApplyErrorCount() {
        return metadataApplyErrorCount;
    }

    public void updateBatchProcessingTime(long elapsedNs) {
        batchProcessingTimeHist.update(NANOSECONDS.toMicros(elapsedNs));
    }

    public void updateBatchSize(int size) {
        batchSizeHist.update(size);
    }

    void updateLastAppliedImageProvenance(MetadataProvenance provenance) {
        lastAppliedImageProvenance.set(provenance);
    }

    long lastAppliedOffset() {
        return lastAppliedImageProvenance.get().lastContainedOffset();
    }

    long lastAppliedTimestamp() {
        return lastAppliedImageProvenance.get().lastContainedLogTimeMs();
    }
}
