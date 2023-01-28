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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.Collections;
import java.util.Locale;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

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
             config.getInt(FETCH_MIN_BYTES_CONFIG),
             config.getInt(FETCH_MAX_BYTES_CONFIG),
             config.getInt(FETCH_MAX_WAIT_MS_CONFIG),
             config.getInt(MAX_PARTITION_FETCH_BYTES_CONFIG),
             config.getInt(MAX_POLL_RECORDS_CONFIG),
             config.getBoolean(CHECK_CRCS_CONFIG),
             config.getString(CLIENT_RACK_CONFIG),
             keyDeserializer,
             valueDeserializer,
             retryBackoffMs,
             requestTimeoutMs,
             isolationLevel,
             metrics,
             metricsRegistry);
    }

    @SuppressWarnings("unchecked")
    public FetchContext(final LogContext logContext,
                        final Time time,
                        final ConsumerConfig config,
                        final Metrics metrics,
                        final FetcherMetricsRegistry metricsRegistry) {
        this.logContext = logContext;
        this.time = time;
        this.minBytes = config.getInt(FETCH_MIN_BYTES_CONFIG);
        this.maxBytes = config.getInt(FETCH_MAX_BYTES_CONFIG);
        this.maxWaitMs = config.getInt(FETCH_MAX_WAIT_MS_CONFIG);
        this.fetchSize = config.getInt(MAX_PARTITION_FETCH_BYTES_CONFIG);
        this.maxPollRecords = config.getInt(MAX_POLL_RECORDS_CONFIG);
        this.checkCrcs = config.getBoolean(CHECK_CRCS_CONFIG);
        this.clientRackId = config.getString(CLIENT_RACK_CONFIG);
        this.keyDeserializer = config.getConfiguredInstance(KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
        this.valueDeserializer = config.getConfiguredInstance(VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
        this.retryBackoffMs = config.getLong(RETRY_BACKOFF_MS_CONFIG);
        this.requestTimeoutMs = config.getInt(REQUEST_TIMEOUT_MS_CONFIG);
        this.isolationLevel = IsolationLevel.valueOf(config.getString(ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));
        this.fetchManagerMetrics = new FetchManagerMetrics(metrics, metricsRegistry);

        String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        this.keyDeserializer.configure(config.originals(Collections.singletonMap(CLIENT_ID_CONFIG, clientId)), true);
        this.valueDeserializer.configure(config.originals(Collections.singletonMap(CLIENT_ID_CONFIG, clientId)), false);
    }

}
