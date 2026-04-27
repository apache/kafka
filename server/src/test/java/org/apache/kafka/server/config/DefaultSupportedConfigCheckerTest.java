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

package org.apache.kafka.server.config;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.server.metrics.ClientMetricsConfigs;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.CLIENT_METRICS;
import static org.apache.kafka.common.config.ConfigResource.Type.GROUP;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultSupportedConfigCheckerTest {
    private final DefaultSupportedConfigChecker checker = new DefaultSupportedConfigChecker();

    @Test
    void testIsSupported() {
        assertTrue(checker.isSupported(TOPIC, TopicConfig.SEGMENT_BYTES_CONFIG));
        assertTrue(checker.isSupported(TOPIC, TopicConfig.SEGMENT_MS_CONFIG));
        assertTrue(checker.isSupported(TOPIC, TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
        assertFalse(checker.isSupported(TOPIC, "invalid.topic.config"));

        assertTrue(checker.isSupported(BROKER, "log.cleaner.threads"));
        assertTrue(checker.isSupported(BROKER, "num.network.threads"));
        assertTrue(checker.isSupported(BROKER, "log.segment.bytes"));
        assertTrue(checker.isSupported(BROKER, "listener.name.EXTERNAL.ssl.keystore.location"));
        assertTrue(checker.isSupported(BROKER, "fake.configurable.authorizer.foobar.config"));

        assertTrue(checker.isSupported(CLIENT_METRICS, ClientMetricsConfigs.PUSH_INTERVAL_MS));
        assertTrue(checker.isSupported(CLIENT_METRICS, ClientMetricsConfigs.SUBSCRIPTION_METRICS));
        assertTrue(checker.isSupported(CLIENT_METRICS, ClientMetricsConfigs.CLIENT_MATCH_PATTERN));
        assertFalse(checker.isSupported(CLIENT_METRICS, "invalid.client.metrics.config"));

        assertTrue(checker.isSupported(GROUP, GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG));
        assertTrue(checker.isSupported(GROUP, GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG));
        assertFalse(checker.isSupported(GROUP, "invalid.group.config"));

        assertTrue(checker.isSupported(TOPIC, QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG));
        assertTrue(checker.isSupported(TOPIC, QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG));

        assertTrue(checker.isSupported(BROKER, QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG));
        assertTrue(checker.isSupported(BROKER, QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG));
        assertTrue(checker.isSupported(BROKER, QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG));
    }
}
