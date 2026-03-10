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
package org.apache.kafka.coordinator.group.modern.share;

import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupConfigManager;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShareGroupConfigProviderTest {
    private ShareGroupConfigProvider provider;

    @Test
    void testRecordLockDurationMsOrDefaultWithGroupConfig() {
        GroupConfigManager groupConfigManager = mock(GroupConfigManager.class);
        GroupConfig groupConfig = mock(GroupConfig.class);
        when(groupConfig.shareRecordLockDurationMs()).thenReturn(1000);
        when(groupConfigManager.groupConfig("test-group")).thenReturn(Optional.of(groupConfig));
        provider = new ShareGroupConfigProvider(groupConfigManager);

        assertEquals(1000, provider.recordLockDurationMsOrDefault("test-group", 100));
    }

    @Test
    void testRecordLockDurationMsOrDefaultWithoutGroupConfig() {
        GroupConfigManager groupConfigManager = mock(GroupConfigManager.class);
        when(groupConfigManager.groupConfig("test-group")).thenReturn(Optional.empty());
        provider = new ShareGroupConfigProvider(groupConfigManager);

        assertEquals(100, provider.recordLockDurationMsOrDefault("test-group", 100));
    }

    @Test
    void testDeliveryCountLimitOrDefaultWithGroupConfig() {
        GroupConfigManager groupConfigManager = mock(GroupConfigManager.class);
        GroupConfig groupConfig = mock(GroupConfig.class);
        when(groupConfig.shareDeliveryCountLimit()).thenReturn(8);
        when(groupConfigManager.groupConfig("test-group")).thenReturn(Optional.of(groupConfig));
        provider = new ShareGroupConfigProvider(groupConfigManager);

        assertEquals(8, provider.deliveryCountLimitOrDefault("test-group", 5));
    }

    @Test
    void testDeliveryCountLimitOrDefaultWithoutGroupConfig() {
        GroupConfigManager groupConfigManager = mock(GroupConfigManager.class);
        when(groupConfigManager.groupConfig("test-group")).thenReturn(Optional.empty());
        provider = new ShareGroupConfigProvider(groupConfigManager);

        assertEquals(5, provider.deliveryCountLimitOrDefault("test-group", 5));
    }

    @Test
    void testPartitionMaxRecordLocksOrDefaultWithGroupConfig() {
        GroupConfigManager groupConfigManager = mock(GroupConfigManager.class);
        GroupConfig groupConfig = mock(GroupConfig.class);
        when(groupConfig.sharePartitionMaxRecordLocks()).thenReturn(5000);
        when(groupConfigManager.groupConfig("test-group")).thenReturn(Optional.of(groupConfig));
        provider = new ShareGroupConfigProvider(groupConfigManager);

        assertEquals(5000, provider.partitionMaxRecordLocksOrDefault("test-group", 2000));
    }

    @Test
    void testPartitionMaxRecordLocksOrDefaultWithoutGroupConfig() {
        GroupConfigManager groupConfigManager = mock(GroupConfigManager.class);
        when(groupConfigManager.groupConfig("test-group")).thenReturn(Optional.empty());
        provider = new ShareGroupConfigProvider(groupConfigManager);

        assertEquals(2000, provider.partitionMaxRecordLocksOrDefault("test-group", 2000));
    }

    @Test
    void testIsRenewAcknowledgeDisabledWithGroupConfig() {
        GroupConfigManager groupConfigManager = mock(GroupConfigManager.class);
        GroupConfig groupConfig = mock(GroupConfig.class);
        when(groupConfig.shareRenewAcknowledgeEnable()).thenReturn(false);
        when(groupConfigManager.groupConfig("test-group")).thenReturn(Optional.of(groupConfig));
        provider = new ShareGroupConfigProvider(groupConfigManager);

        assertFalse(provider.isRenewAcknowledgeEnabled("test-group"));
    }

    @Test
    void testIsRenewAcknowledgeEnabledWithoutGroupConfig() {
        GroupConfigManager groupConfigManager = mock(GroupConfigManager.class);
        when(groupConfigManager.groupConfig("test-group")).thenReturn(Optional.empty());
        provider = new ShareGroupConfigProvider(groupConfigManager);

        assertTrue(provider.isRenewAcknowledgeEnabled("test-group"));
    }
}
