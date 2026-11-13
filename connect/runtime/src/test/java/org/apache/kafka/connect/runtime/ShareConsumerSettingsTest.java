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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShareConsumerSettingsTest {

    @Test
    void preservesCompatibleClientLayersAndEnforcesBoundedExplicitDelivery() {
        Map<String, Object> common = new HashMap<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.CLIENT_ID_CONFIG, "share-sink-task-0",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL",
                "metrics.context.connect.kafka.cluster.id", "cluster-id"));
        Map<String, Object> worker = new HashMap<>(Map.of(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100",
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1048576",
                ConsumerConfig.GROUP_ID_CONFIG, "orders-share"));
        Map<String, Object> connector = new HashMap<>(Map.of(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "25",
                ConsumerConfig.GROUP_ID_CONFIG, "orders-share",
                ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, " explicit ",
                ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, " RECORD_LIMIT "));
        Map<String, Object> commonBefore = Map.copyOf(common);
        Map<String, Object> workerBefore = Map.copyOf(worker);
        Map<String, Object> connectorBefore = Map.copyOf(connector);

        Map<String, Object> result = ShareConsumerSettings.create("orders",
                Map.of(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG, "orders-share"), common, worker, connector);

        assertEquals("orders-share", result.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("explicit", result.get(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG));
        assertEquals("record_limit", result.get(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG));
        assertEquals("25", result.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals("1048576", result.get(ConsumerConfig.FETCH_MAX_BYTES_CONFIG));
        common.forEach((key, value) -> assertEquals(value, result.get(key), key));
        assertEquals("0", result.get(CommonClientConfigs.BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG));
        assertFalse(result.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertFalse(result.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertFalse(result.containsKey(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG));
        assertEquals(commonBefore, common);
        assertEquals(workerBefore, worker);
        assertEquals(connectorBefore, connector);
    }

    @Test
    void resolvesOneGroupIdentityAndRejectsHiddenConflicts() {
        assertEquals("connect-share-orders", ShareConsumerSettings.resolveGroupId(" orders ", Map.of()));
        assertThrows(ConfigException.class, () -> ShareConsumerSettings.resolveGroupId(null, Map.of()));
        assertEquals("orders-share", ShareConsumerSettings.resolveGroupId("orders",
                Map.of(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG, " orders-share ")));
        assertEquals("orders-share", ShareConsumerSettings.resolveGroupId(null,
                Map.of(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG, "orders-share")));
        assertThrows(ConfigException.class, () -> ShareConsumerSettings.resolveGroupId("orders",
                Map.of(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG, " ")));
        assertThrows(ConfigException.class, () -> ShareConsumerSettings.resolveGroupId(null, Map.of()));

        Map<String, Object> canonical = Map.of(SinkConnectorConfig.SHARE_GROUP_ID_CONFIG, "orders-share");
        Map<String, Object> matching = Map.of(ConsumerConfig.GROUP_ID_CONFIG, "orders-share");
        Map<String, Object> conflicting = Map.of(ConsumerConfig.GROUP_ID_CONFIG, "old-offset-group");
        assertThrows(ConfigException.class, () -> ShareConsumerSettings.create(
                "orders", canonical, Map.of(), conflicting, matching));
        assertThrows(ConfigException.class, () -> ShareConsumerSettings.create(
                "orders", canonical, Map.of(), matching, conflicting));
        assertThrows(ConfigException.class, () -> ShareConsumerSettings.create(
                "orders", Map.of(), Map.of(), Map.of(), matching));
        assertTrue(ShareConsumerSettings.validationErrors(matching, null).isEmpty());
        assertFalse(ShareConsumerSettings.validationErrors(Map.of(ConsumerConfig.GROUP_ID_CONFIG, " "), null).isEmpty());
    }

    @Test
    void collectsUnsafeDeclarationsAndRejectsThemBeforeOverrideMerging() {
        Map<String, Object> unsafe = new HashMap<>(Map.ofEntries(
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                Map.entry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"),
                Map.entry(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance"),
                Map.entry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"),
                Map.entry(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "assignor"),
                Map.entry(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "interceptor"),
                Map.entry(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"),
                Map.entry(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000"),
                Map.entry(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer"),
                Map.entry(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "uniform"),
                Map.entry(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "implicit"),
                Map.entry(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "batch_optimized"),
                Map.entry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "-1")));
        unsafe.put(ConsumerConfig.GROUP_ID_CONFIG, null);
        Map<String, String> errors = ShareConsumerSettings.validationErrors(unsafe, "connect-share-orders");
        assertEquals(unsafe.keySet(), errors.keySet());
        assertTrue(errors.get(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG).contains("explicit"));
        assertTrue(errors.get(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG).contains("record_limit"));
        assertThrows(UnsupportedOperationException.class, () -> errors.put("new-key", "error"));
        assertEquals(Set.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG),
                ShareConsumerSettings.validationErrors(Map.of(
                        ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "EXPLICIT"), null).keySet());

        Map<String, Object> safeModes = Map.of(
                ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit",
                ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
        assertThrows(ConfigException.class, () -> ShareConsumerSettings.create(
                "orders", Map.of(), Map.of(), unsafe, safeModes));
        assertThrows(ConfigException.class, () -> ShareConsumerSettings.create("orders", Map.of(),
                Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"), Map.of(), Map.of()));
        assertThrows(ConfigException.class, () -> ShareConsumerSettings.create(
                "orders", Map.of(), Map.of(), Map.of(), unsafe));
    }
}
