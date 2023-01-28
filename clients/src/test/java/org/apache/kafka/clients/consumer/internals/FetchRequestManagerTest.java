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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollContext;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class FetchRequestManagerTest {
    private MockTime time;
    private ErrorEventHandler errorEventHandler;
    @BeforeEach
    public void setup() {
        this.time = new MockTime();
        this.errorEventHandler = mock(ErrorEventHandler.class);
    }

    @Test
    public void testSuccessfulResponse() {
        LogContext logContext = new LogContext("[testSuccessfulResponse] ");
        ConsumerConfig config = newConfig();
        OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(config.getString(AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
        SubscriptionState subscriptions = new SubscriptionState(logContext, offsetResetStrategy);

        try (ConsumerMetadata metadata = newMetadata(config, subscriptions, logContext); Metrics metrics = new Metrics()) {
            FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry();
            FetchContext<byte[], byte[]> fetchContext = new FetchContext<>(
                    logContext,
                    time,
                    config,
                    metrics,
                    metricsRegistry);
            ApiVersions apiVersions = new ApiVersions();
            RequestState requestState = new RequestState(fetchContext.retryBackoffMs);
            FetchRequestManager<byte[], byte[]> fetchRequestManager = new FetchRequestManager<>(fetchContext, apiVersions, this.errorEventHandler, requestState);

            try {
                TopicPartition tp0 = new TopicPartition("test", 0);
                Set<TopicPartition> partitions = Collections.singleton(tp0);

                if (!subscriptions.assignFromUser(partitions))
                    fail(format("Subscriptions in %s should be different from those in %s", subscriptions, partitions));

//                subscriptions.resetInitializingPositions();
                subscriptions.seek(tp0, 0);

                PollContext pollContext = new PollContext(metadata, subscriptions, fetchContext.time.milliseconds());
                PollResult pollResult = fetchRequestManager.poll(pollContext);
                assertEquals(Collections.emptyList(), pollResult.unsentRequests);
            } finally {
                fetchRequestManager.close(subscriptions);
            }
        }
    }

    private ConsumerConfig newConfig() {
        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new ConsumerConfig(props);
    }

    private ConsumerMetadata newMetadata(ConsumerConfig config, SubscriptionState subscriptions, LogContext logContext) {
        return new ConsumerMetadata(
                config.getLong(RETRY_BACKOFF_MS_CONFIG),
                config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                subscriptions,
                logContext,
                new ClusterResourceListeners());
    }
}
