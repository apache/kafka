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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

public class FetcherThreadSafeTest extends FetcherTest {

    protected <K, V> Fetcher<K, V> buildFetcher(LogContext logContext,
                                                ConsumerNetworkClient client,
                                                int minBytes,
                                                int maxBytes,
                                                int maxWaitMs,
                                                int fetchSize,
                                                int maxPollRecords,
                                                boolean checkCrcs,
                                                String clientRackId,
                                                Deserializer<K> keyDeserializer,
                                                Deserializer<V> valueDeserializer,
                                                ConsumerMetadata metadata,
                                                SubscriptionState subscriptions,
                                                Metrics metrics,
                                                FetcherMetricsRegistry metricsRegistry,
                                                MockTime time,
                                                long retryBackoffMs,
                                                long requestTimeoutMs,
                                                IsolationLevel isolationLevel,
                                                ApiVersions apiVersions) {
        return new FetcherThreadSafe<>(new LogContext(),
                                       client,
                                       minBytes,
                                       maxBytes,
                                       maxWaitMs,
                                       fetchSize,
                                       maxPollRecords,
                                       checkCrcs,
                                       clientRackId,
                                       keyDeserializer,
                                       valueDeserializer,
                                       metadata,
                                       subscriptions,
                                       metrics,
                                       metricsRegistry,
                                       time,
                                       retryBackoffMs,
                                       requestTimeoutMs,
                                       isolationLevel,
                                       apiVersions);
    }
}
