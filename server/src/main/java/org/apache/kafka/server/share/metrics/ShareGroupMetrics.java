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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ShareGroupMetrics is used to track the broker-side metrics for the ShareGroup.
 */
public class ShareGroupMetrics implements AutoCloseable {
    // Rate of records acknowledged per acknowledgement type.
    private static final String RECORD_ACKNOWLEDGEMENTS_PER_SEC = "RecordAcknowledgementsPerSec";
    // The time in milliseconds to load the share partitions.
    private static final String PARTITION_LOAD_TIME_MS = "PartitionLoadTimeMs";
    private static final String ACK_TYPE_TAG = "ackType";

    private final KafkaMetricsGroup metricsGroup;
    private final Time time;
    private final Map<Byte, Meter> recordAcknowledgementMeterMap;
    private final Histogram partitionLoadTimeMs;

    public ShareGroupMetrics(Time time) {
        this.time = time;
        this.metricsGroup = new KafkaMetricsGroup("kafka.server", "ShareGroupMetrics");
        this.recordAcknowledgementMeterMap = Arrays.stream(AcknowledgeType.values()).collect(
            Collectors.toMap(
                type -> type.id,
                type -> metricsGroup.newMeter(
                    RECORD_ACKNOWLEDGEMENTS_PER_SEC,
                    "records",
                    TimeUnit.SECONDS,
                    Map.of(ACK_TYPE_TAG, capitalize(type.toString()))
                )
            )
        );
        partitionLoadTimeMs = metricsGroup.newHistogram(PARTITION_LOAD_TIME_MS);
    }

    public void recordAcknowledgement(byte ackType) {
        // unknown ack types (such as gaps for control records) are intentionally ignored
        if (recordAcknowledgementMeterMap.containsKey(ackType)) {
            recordAcknowledgementMeterMap.get(ackType).mark();
        }
    }

    public void partitionLoadTime(long start) {
        partitionLoadTimeMs.update(time.hiResClockMs() - start);
    }

    public Meter recordAcknowledgementMeter(byte ackType) {
        return recordAcknowledgementMeterMap.get(ackType);
    }

    public Histogram partitionLoadTimeMs() {
        return partitionLoadTimeMs;
    }

    @Override
    public void close() throws Exception {
        Arrays.stream(AcknowledgeType.values()).forEach(
            m -> metricsGroup.removeMetric(RECORD_ACKNOWLEDGEMENTS_PER_SEC, Map.of(ACK_TYPE_TAG, m.toString())));
        metricsGroup.removeMetric(PARTITION_LOAD_TIME_MS);
    }

    private static String capitalize(String string) {
        if (string == null || string.isEmpty()) {
            return string;
        }
        return string.substring(0, 1).toUpperCase(Locale.ROOT) + string.substring(1);
    }
}
