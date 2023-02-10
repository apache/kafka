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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class will process records containing timestamps and generate a histogram based on the data.  It will then be
 * present in the status from the `ConsumeBenchWorker` class.  This must be used with a timestamped PayloadGenerator
 * implementation.
 *
 * Example spec:
 * {
 *    "type": "timestamp",
 *    "histogramMaxMs": 10000,
 *    "histogramMinMs": 0,
 *    "histogramStepMs": 1
 * }
 *
 * This will track total E2E latency up to 10 seconds, using 1ms resolution and a timestamp size of 8 bytes.
 */

public class TimestampRecordProcessor implements RecordProcessor {
    private final int histogramMaxMs;
    private final int histogramMinMs;
    private final int histogramStepMs;
    private final ByteBuffer buffer;
    private final Histogram histogram;

    private final Logger log = LoggerFactory.getLogger(TimestampRecordProcessor.class);

    final static float[] PERCENTILES = {0.5f, 0.95f, 0.99f};

    @JsonCreator
    public TimestampRecordProcessor(@JsonProperty("histogramMaxMs") int histogramMaxMs,
                                    @JsonProperty("histogramMinMs") int histogramMinMs,
                                    @JsonProperty("histogramStepMs") int histogramStepMs) {
        this.histogramMaxMs = histogramMaxMs;
        this.histogramMinMs = histogramMinMs;
        this.histogramStepMs = histogramStepMs;
        this.histogram = new Histogram((histogramMaxMs - histogramMinMs) / histogramStepMs);
        buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @JsonProperty
    public int histogramMaxMs() {
        return histogramMaxMs;
    }

    @JsonProperty
    public int histogramMinMs() {
        return histogramMinMs;
    }

    @JsonProperty
    public int histogramStepMs() {
        return histogramStepMs;
    }

    private void putHistogram(long latency) {
        histogram.add(Long.max(0L, (latency - histogramMinMs) / histogramStepMs));
    }

    @Override
    public synchronized void processRecords(ConsumerRecords<byte[], byte[]> consumerRecords) {
        // Save the current time to prevent skew by processing time.
        long curTime = Time.SYSTEM.milliseconds();
        for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
            try {
                buffer.clear();
                buffer.put(record.value(), 0, Long.BYTES);
                buffer.rewind();
                putHistogram(curTime - buffer.getLong());
            } catch (RuntimeException e) {
                log.error("Error in processRecords:", e);
            }
        }
    }

    @Override
    public JsonNode processorStatus() {
        Histogram.Summary summary = histogram.summarize(PERCENTILES);
        StatusData statusData = new StatusData(
                summary.average() * histogramStepMs + histogramMinMs,
                summary.percentiles().get(0).value() * histogramStepMs + histogramMinMs,
                summary.percentiles().get(1).value() * histogramStepMs + histogramMinMs,
                summary.percentiles().get(2).value() * histogramStepMs + histogramMinMs);
        return JsonUtil.JSON_SERDE.valueToTree(statusData);
    }

    private static class StatusData {
        private final float averageLatencyMs;
        private final int p50LatencyMs;
        private final int p95LatencyMs;
        private final int p99LatencyMs;

        /**
         * The percentiles to use when calculating the histogram data.
         * These should match up with the p50LatencyMs, p95LatencyMs, etc. fields.
         */
        final static float[] PERCENTILES = {0.5f, 0.95f, 0.99f};

        @JsonCreator
        StatusData(@JsonProperty("averageLatencyMs") float averageLatencyMs,
                   @JsonProperty("p50LatencyMs") int p50latencyMs,
                   @JsonProperty("p95LatencyMs") int p95latencyMs,
                   @JsonProperty("p99LatencyMs") int p99latencyMs) {
            this.averageLatencyMs = averageLatencyMs;
            this.p50LatencyMs = p50latencyMs;
            this.p95LatencyMs = p95latencyMs;
            this.p99LatencyMs = p99latencyMs;
        }

        @JsonProperty
        public float averageLatencyMs() {
            return averageLatencyMs;
        }

        @JsonProperty
        public int p50LatencyMs() {
            return p50LatencyMs;
        }

        @JsonProperty
        public int p95LatencyMs() {
            return p95LatencyMs;
        }

        @JsonProperty
        public int p99LatencyMs() {
            return p99LatencyMs;
        }
    }
}
