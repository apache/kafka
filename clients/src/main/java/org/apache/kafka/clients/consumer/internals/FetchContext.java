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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

public class FetchContext<K, V> {

    final LogContext logContext;
    final Time time;
    final int minBytes;
    final int maxBytes;
    final int maxWaitMs;
    final int fetchSize;
    final long retryBackoffMs;
    final long requestTimeoutMs;
    final int maxPollRecords;
    final boolean checkCrcs;
    final String clientRackId;
    final BufferSupplier decompressionBufferSupplier = BufferSupplier.create();
    final Deserializer<K> keyDeserializer;
    final Deserializer<V> valueDeserializer;
    final IsolationLevel isolationLevel;
    final FetchManagerMetrics fetchManagerMetrics;

    public FetchContext(final LogContext logContext,
                        final Time time,
                        final int minBytes,
                        final int maxBytes,
                        final int maxWaitMs,
                        final int fetchSize,
                        final int maxPollRecords,
                        final boolean checkCrcs,
                        final String clientRackId,
                        final Deserializer<K> keyDeserializer,
                        final Deserializer<V> valueDeserializer,
                        final long retryBackoffMs,
                        final long requestTimeoutMs,
                        final IsolationLevel isolationLevel,
                        final Metrics metrics,
                        final FetcherMetricsRegistry metricsRegistry) {
        this.logContext = logContext;
        this.time = time;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.checkCrcs = checkCrcs;
        this.clientRackId = clientRackId;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.isolationLevel = isolationLevel;
        this.fetchManagerMetrics = new FetchManagerMetrics(metrics, metricsRegistry);
    }

    public FetchContext(final LogContext logContext,
                        final Time time,
                        final ConsumerConfig config,
                        final Deserializer<K> keyDeserializer,
                        final Deserializer<V> valueDeserializer,
                        final long retryBackoffMs,
                        final long requestTimeoutMs,
                        final IsolationLevel isolationLevel,
                        final Metrics metrics,
                        final FetcherMetricsRegistry metricsRegistry) {
        this(logContext,
             time,
             config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
             config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
             config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG),
             config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
             config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
             config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG),
             config.getString(ConsumerConfig.CLIENT_RACK_CONFIG),
             keyDeserializer,
             valueDeserializer,
             retryBackoffMs,
             requestTimeoutMs,
             isolationLevel,
             metrics,
             metricsRegistry);
    }

    public Logger logger(Class<?> clazz) {
        return logContext.logger(clazz);
    }

}
