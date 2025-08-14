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
package org.apache.kafka.storage.log.metrics;

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Partition-level throughput metrics for a topic partition.
 * This intentionally holds a minimal subset of metrics focused on producer/consumer throughput
 * as described in KIP-977.
 */
public final class BrokerTopicPartitionMetrics {
    public static final String MESSAGE_IN_PER_SEC = "MessagesInPerSec";
    public static final String BYTES_IN_PER_SEC = "BytesInPerSec";
    public static final String BYTES_OUT_PER_SEC = "BytesOutPerSec";
    public static final String BYTES_REJECTED_PER_SEC = "BytesRejectedPerSec";
    public static final String TOTAL_PRODUCE_REQUESTS_PER_SEC = "TotalProduceRequestsPerSec";
    public static final String TOTAL_FETCH_REQUESTS_PER_SEC = "TotalFetchRequestsPerSec";
    public static final String FAILED_PRODUCE_REQUESTS_PER_SEC = "FailedProduceRequestsPerSec";
    public static final String FAILED_FETCH_REQUESTS_PER_SEC = "FailedFetchRequestsPerSec";
    public static final String FETCH_MESSAGE_CONVERSIONS_PER_SEC = "FetchMessageConversionsPerSec";
    public static final String PRODUCE_MESSAGE_CONVERSIONS_PER_SEC = "ProduceMessageConversionsPerSec";

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "BrokerTopicPartitionMetrics");
    private final Map<String, String> tags;

    private final MeterWrapper messagesInRate;
    private final MeterWrapper bytesInRate;
    private final MeterWrapper bytesOutRate;
    private final MeterWrapper bytesRejectedRate;
    private final MeterWrapper totalProduceRequestRate;
    private final MeterWrapper totalFetchRequestRate;
    private final MeterWrapper failedProduceRequestRate;
    private final MeterWrapper failedFetchRequestRate;
    private final MeterWrapper fetchMessageConversionsRate;
    private final MeterWrapper produceMessageConversionsRate;

    public BrokerTopicPartitionMetrics(String topic, int partition) {
        this.tags = Map.of("topic", topic, "partition", Integer.toString(partition));
        this.messagesInRate = new MeterWrapper(MESSAGE_IN_PER_SEC, "messages");
        this.bytesInRate = new MeterWrapper(BYTES_IN_PER_SEC, "bytes");
        this.bytesOutRate = new MeterWrapper(BYTES_OUT_PER_SEC, "bytes");
        this.bytesRejectedRate = new MeterWrapper(BYTES_REJECTED_PER_SEC, "bytes");
        this.totalProduceRequestRate = new MeterWrapper(TOTAL_PRODUCE_REQUESTS_PER_SEC, "requests");
        this.totalFetchRequestRate = new MeterWrapper(TOTAL_FETCH_REQUESTS_PER_SEC, "requests");
        this.failedProduceRequestRate = new MeterWrapper(FAILED_PRODUCE_REQUESTS_PER_SEC, "requests");
        this.failedFetchRequestRate = new MeterWrapper(FAILED_FETCH_REQUESTS_PER_SEC, "requests");
        this.fetchMessageConversionsRate = new MeterWrapper(FETCH_MESSAGE_CONVERSIONS_PER_SEC, "requests");
        this.produceMessageConversionsRate = new MeterWrapper(PRODUCE_MESSAGE_CONVERSIONS_PER_SEC, "requests");
    }

    public Meter messagesInRate() { return messagesInRate.meter(); }
    public Meter bytesInRate() { return bytesInRate.meter(); }
    public Meter bytesOutRate() { return bytesOutRate.meter(); }
    public Meter bytesRejectedRate() { return bytesRejectedRate.meter(); }
    public Meter totalProduceRequestRate() { return totalProduceRequestRate.meter(); }
    public Meter totalFetchRequestRate() { return totalFetchRequestRate.meter(); }
    public Meter failedProduceRequestRate() { return failedProduceRequestRate.meter(); }
    public Meter failedFetchRequestRate() { return failedFetchRequestRate.meter(); }
    public Meter fetchMessageConversionsRate() { return fetchMessageConversionsRate.meter(); }
    public Meter produceMessageConversionsRate() { return produceMessageConversionsRate.meter(); }

    public void close() {
        messagesInRate.close();
        bytesInRate.close();
        bytesOutRate.close();
        bytesRejectedRate.close();
        totalProduceRequestRate.close();
        totalFetchRequestRate.close();
        failedProduceRequestRate.close();
        failedFetchRequestRate.close();
        fetchMessageConversionsRate.close();
        produceMessageConversionsRate.close();
    }

    private final class MeterWrapper {
        private final String metricType;
        private final String eventType;
        private volatile Meter lazyMeter;
        private final Lock meterLock = new ReentrantLock();

        MeterWrapper(String metricType, String eventType) {
            this.metricType = metricType;
            this.eventType = eventType;
        }

        Meter meter() {
            Meter meter = lazyMeter;
            if (meter == null) {
                meterLock.lock();
                try {
                    meter = lazyMeter;
                    if (meter == null) {
                        meter = metricsGroup.newMeter(metricType, eventType, TimeUnit.SECONDS, tags);
                        lazyMeter = meter;
                    }
                } finally {
                    meterLock.unlock();
                }
            }
            return meter;
        }

        void close() {
            meterLock.lock();
            try {
                if (lazyMeter != null) {
                    metricsGroup.removeMetric(metricType, tags);
                    lazyMeter = null;
                }
            } finally {
                meterLock.unlock();
            }
        }
    }
}


