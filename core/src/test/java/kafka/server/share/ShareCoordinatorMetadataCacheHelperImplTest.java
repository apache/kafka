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

package kafka.server.share;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.dlq.ShareGroupDLQMetadataCacheHelper;
import org.apache.kafka.server.share.persister.ShareCoordinatorMetadataCacheHelper;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareCoordinatorMetadataCacheHelperImplTest {
    @Test
    public void testConstructorThrowsErrorOnNullArgs() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;

        Exception e = assertThrows(NullPointerException.class, () -> new ShareCoordinatorMetadataCacheHelperImpl(
            null,
            func,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        ));
        assertEquals("metadataCache must not be null", e.getMessage());

        e = assertThrows(NullPointerException.class, () -> new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            null,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        ));
        assertEquals("keyToPartitionMapper must not be null", e.getMessage());

        e = assertThrows(NullPointerException.class, () -> new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            func,
            null,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        ));
        assertEquals("interBrokerListenerName must not be null", e.getMessage());

        e = assertThrows(NullPointerException.class, () -> new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            func,
            mock(ListenerName.class),
            null,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        ));
        assertEquals("groupConfigManager must not be null", e.getMessage());

        e = assertThrows(NullPointerException.class, () -> new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            func,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            null
        ));
        assertEquals("messageMaxBytesSupplier must not be null", e.getMessage());
    }

    @Test
    public void testContainsTopicReturnsFalseOnException() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        when(mockMetadataCache.contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME)))
            .thenReturn(false);

        ShareCoordinatorMetadataCacheHelper cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            func,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        when(mockMetadataCache.contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME)))
            .thenThrow(
                new RuntimeException("bad stuff")
            );

        assertFalse(cache.containsTopic(Topic.SHARE_GROUP_STATE_TOPIC_NAME));
        verify(mockMetadataCache, times(1)).contains(Topic.SHARE_GROUP_STATE_TOPIC_NAME);
    }

    @Test
    public void testContainsTopicSuccess() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        when(mockMetadataCache.contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME)))
            .thenReturn(false);

        ShareCoordinatorMetadataCacheHelper cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            func,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        when(mockMetadataCache.contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME)))
            .thenReturn(
                true
            );

        assertTrue(cache.containsTopic(Topic.SHARE_GROUP_STATE_TOPIC_NAME));
        verify(mockMetadataCache, times(1)).contains(Topic.SHARE_GROUP_STATE_TOPIC_NAME);
    }

    @Test
    public void testShareCoordinatorReturnsNoNodeWhenNoInternalTopicInCache() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        when(mockMetadataCache.contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME)))
            .thenReturn(false);

        ShareCoordinatorMetadataCacheHelper cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            func,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertEquals(
            Node.noNode(),
            cache.getShareCoordinator(SharePartitionKey.getInstance("group", Uuid.randomUuid(), 0), Topic.SHARE_GROUP_STATE_TOPIC_NAME)
        );

        verify(mockMetadataCache, times(1)).contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME));
    }

    @Test
    public void testShareCoordinatorReturnsNoNodeIfTopicMetadataInvalid() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);

        ShareCoordinatorMetadataCacheHelper cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            func,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        when(mockMetadataCache.contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME)))
            .thenReturn(true);

        // null topic metadata response
        when(mockMetadataCache.getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        )).thenReturn(
            null
        );

        assertEquals(
            Node.noNode(),
            cache.getShareCoordinator(SharePartitionKey.getInstance("group", Uuid.randomUuid(), 0), Topic.SHARE_GROUP_STATE_TOPIC_NAME)
        );

        verify(mockMetadataCache, times(1)).contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME));
        verify(mockMetadataCache, times(1)).getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        );

        // empty topic metadata response
        when(mockMetadataCache.getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        )).thenReturn(
            List.of()
        );

        assertEquals(
            Node.noNode(),
            cache.getShareCoordinator(SharePartitionKey.getInstance("group", Uuid.randomUuid(), 0), Topic.SHARE_GROUP_STATE_TOPIC_NAME)
        );

        verify(mockMetadataCache, times(2)).contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME));
        verify(mockMetadataCache, times(2)).getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        );

        // erroneous topic metadata response
        when(mockMetadataCache.getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        )).thenReturn(
            List.of(
                new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode(Errors.BROKER_NOT_AVAILABLE.code())
            )
        );

        assertEquals(
            Node.noNode(),
            cache.getShareCoordinator(SharePartitionKey.getInstance("group", Uuid.randomUuid(), 0), Topic.SHARE_GROUP_STATE_TOPIC_NAME)
        );

        verify(mockMetadataCache, times(3)).contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME));
        verify(mockMetadataCache, times(3)).getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        );
    }

    @Test
    public void testShareCoordinatorReturnsNoNodeOnGetAliveNodeEmptyResponse() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);

        ShareCoordinatorMetadataCacheHelper cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            func,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        when(mockMetadataCache.contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME)))
            .thenReturn(true);

        // correct topic metadata response
        when(mockMetadataCache.getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        )).thenReturn(
            List.of(
                new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode(Errors.NONE.code())
                    .setPartitions(List.of(
                        new MetadataResponseData.MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setLeaderId(1)
                    ))
            )
        );

        // get alive broker node throws exception
        when(mockMetadataCache.getAliveBrokerNode(
            eq(1),
            eq(mockListenerName)
        )).thenReturn(
            Optional.empty()
        );

        assertEquals(
            Node.noNode(),
            cache.getShareCoordinator(SharePartitionKey.getInstance("group", Uuid.randomUuid(), 0), Topic.SHARE_GROUP_STATE_TOPIC_NAME)
        );

        verify(mockMetadataCache, times(1)).contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME));
        verify(mockMetadataCache, times(1)).getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        );
        verify(mockMetadataCache, times(1)).getAliveBrokerNode(eq(1), eq(mockListenerName));
    }

    @Test
    public void testShareCoordinatorReturnsNoNodeOnGetAliveNodeException() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);

        ShareCoordinatorMetadataCacheHelper cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            func,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        when(mockMetadataCache.contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME)))
            .thenReturn(true);

        // correct topic metadata response
        when(mockMetadataCache.getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        )).thenReturn(
            List.of(
                new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode(Errors.NONE.code())
                    .setPartitions(List.of(
                        new MetadataResponseData.MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setLeaderId(1)
                    ))
            )
        );

        // get alive broker node throws exception
        when(mockMetadataCache.getAliveBrokerNode(
            eq(1),
            eq(mockListenerName)
        )).thenThrow(
            new CoordinatorNotAvailableException("bad stuff")
        );

        assertEquals(
            Node.noNode(),
            cache.getShareCoordinator(SharePartitionKey.getInstance("group", Uuid.randomUuid(), 0), Topic.SHARE_GROUP_STATE_TOPIC_NAME)
        );

        verify(mockMetadataCache, times(1)).contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME));
        verify(mockMetadataCache, times(1)).getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        );
        verify(mockMetadataCache, times(1)).getAliveBrokerNode(eq(1), eq(mockListenerName));
    }

    @Test
    public void testShareCoordinatorSuccess() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);

        ShareCoordinatorMetadataCacheHelper cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            func,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        when(mockMetadataCache.contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME)))
            .thenReturn(true);

        // correct topic metadata response
        when(mockMetadataCache.getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        )).thenReturn(
            List.of(
                new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode(Errors.NONE.code())
                    .setPartitions(List.of(
                        new MetadataResponseData.MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setLeaderId(1)
                    ))
            )
        );

        // get alive broker node throws exception
        Node node = new Node(2, "some.domain.name", 65534);
        when(mockMetadataCache.getAliveBrokerNode(
            eq(1),
            eq(mockListenerName)
        )).thenReturn(
            Optional.of(node)
        );

        assertEquals(
            node,
            cache.getShareCoordinator(SharePartitionKey.getInstance("group", Uuid.randomUuid(), 0), Topic.SHARE_GROUP_STATE_TOPIC_NAME)
        );

        verify(mockMetadataCache, times(1)).contains(eq(Topic.SHARE_GROUP_STATE_TOPIC_NAME));
        verify(mockMetadataCache, times(1)).getTopicMetadata(
            any(),
            eq(mockListenerName),
            eq(false),
            eq(false)
        );
        verify(mockMetadataCache, times(1)).getAliveBrokerNode(eq(1), eq(mockListenerName));
    }

    @Test
    public void testGetClusterNodesEmptyListOnException() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);

        ShareCoordinatorMetadataCacheHelper cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            func,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        when(mockMetadataCache.getAliveBrokerNodes(
            eq(mockListenerName)
        )).thenThrow(
            new CoordinatorNotAvailableException("scary stuff")
        );

        assertEquals(
            List.of(),
            cache.getClusterNodes()
        );

        verify(mockMetadataCache, times(1)).getAliveBrokerNodes(eq(mockListenerName));
    }

    @Test
    public void testGetClusterNodesSuccess() {
        Function<SharePartitionKey, Integer> func = sharePartitionKey -> 0;
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);

        ShareCoordinatorMetadataCacheHelper cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            func,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        List<Node> nodes = List.of(
            new Node(0, "some.domain.name", 65534),
            new Node(1, "some.domain.name", 12345)
        );

        when(mockMetadataCache.getAliveBrokerNodes(
            eq(mockListenerName)
        )).thenReturn(
            nodes
        );

        assertEquals(
            nodes,
            cache.getClusterNodes()
        );

        verify(mockMetadataCache, times(1)).getAliveBrokerNodes(eq(mockListenerName));
    }

    // Tests for shareGroupDlqTopic

    @Test
    public void testShareGroupDlqTopicReturnsEmptyWhenGroupConfigIsEmpty() {
        GroupConfigManager mockGroupConfigManager = mock(GroupConfigManager.class);
        when(mockGroupConfigManager.groupConfig("test-group")).thenReturn(Optional.empty());

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mockGroupConfigManager,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertEquals(Optional.empty(), cache.shareGroupDlqTopic("test-group"));
        verify(mockGroupConfigManager, times(1)).groupConfig("test-group");
    }

    @Test
    public void testShareGroupDlqTopicReturnsTopicName() {
        GroupConfigManager mockGroupConfigManager = mock(GroupConfigManager.class);
        GroupConfig mockGroupConfig = mock(GroupConfig.class);
        when(mockGroupConfig.errorsDLQTopicName()).thenReturn("dlq-topic");
        when(mockGroupConfigManager.groupConfig("test-group")).thenReturn(Optional.of(mockGroupConfig));

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mockGroupConfigManager,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertEquals(Optional.of("dlq-topic"), cache.shareGroupDlqTopic("test-group"));
        verify(mockGroupConfigManager, times(1)).groupConfig("test-group");
    }

    // Tests for isShareGroupDlqCopyRecordEnabled

    @Test
    public void testIsShareGroupDlqCopyRecordEnabledReturnsFalseWhenGroupConfigEmpty() {
        GroupConfigManager mockGroupConfigManager = mock(GroupConfigManager.class);
        when(mockGroupConfigManager.groupConfig("test-group")).thenReturn(Optional.empty());

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mockGroupConfigManager,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertFalse(cache.isShareGroupDlqCopyRecordEnabled("test-group"));
        verify(mockGroupConfigManager, times(1)).groupConfig("test-group");
    }

    @Test
    public void testIsShareGroupDlqCopyRecordEnabledReturnsTrue() {
        GroupConfigManager mockGroupConfigManager = mock(GroupConfigManager.class);
        GroupConfig mockGroupConfig = mock(GroupConfig.class);
        when(mockGroupConfig.errorsDLQCopyRecordEnable()).thenReturn(true);
        when(mockGroupConfigManager.groupConfig("test-group")).thenReturn(Optional.of(mockGroupConfig));

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mockGroupConfigManager,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertTrue(cache.isShareGroupDlqCopyRecordEnabled("test-group"));
        verify(mockGroupConfigManager, times(1)).groupConfig("test-group");
    }

    @Test
    public void testIsShareGroupDlqCopyRecordEnabledReturnsFalse() {
        GroupConfigManager mockGroupConfigManager = mock(GroupConfigManager.class);
        GroupConfig mockGroupConfig = mock(GroupConfig.class);
        when(mockGroupConfig.errorsDLQCopyRecordEnable()).thenReturn(false);
        when(mockGroupConfigManager.groupConfig("test-group")).thenReturn(Optional.of(mockGroupConfig));

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mockGroupConfigManager,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertFalse(cache.isShareGroupDlqCopyRecordEnabled("test-group"));
        verify(mockGroupConfigManager, times(1)).groupConfig("test-group");
    }

    // Tests for isDlqAutoTopicCreateEnabled

    @Test
    public void testIsDlqAutoTopicCreateEnabledReturnsTrue() {
        GroupConfigManager mockGroupConfigManager = mock(GroupConfigManager.class);
        when(mockGroupConfigManager.isDlqAutoTopicCreateEnabled()).thenReturn(true);

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mockGroupConfigManager,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertTrue(cache.isDlqAutoTopicCreateEnabled());
        verify(mockGroupConfigManager, times(1)).isDlqAutoTopicCreateEnabled();
    }

    @Test
    public void testIsDlqAutoTopicCreateEnabledReturnsFalse() {
        GroupConfigManager mockGroupConfigManager = mock(GroupConfigManager.class);
        when(mockGroupConfigManager.isDlqAutoTopicCreateEnabled()).thenReturn(false);

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mockGroupConfigManager,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertFalse(cache.isDlqAutoTopicCreateEnabled());
        verify(mockGroupConfigManager, times(1)).isDlqAutoTopicCreateEnabled();
    }

    // Tests for shareGroupDlqTopicPrefix

    @Test
    public void testShareGroupDlqTopicPrefixReturnsPrefix() {
        GroupConfigManager mockGroupConfigManager = mock(GroupConfigManager.class);
        when(mockGroupConfigManager.shareGroupDlqTopicPrefix()).thenReturn(Optional.of("dlq."));

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mockGroupConfigManager,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertEquals(Optional.of("dlq."), cache.shareGroupDlqTopicPrefix());
        verify(mockGroupConfigManager, times(1)).shareGroupDlqTopicPrefix();
    }

    @Test
    public void testShareGroupDlqTopicPrefixReturnsEmpty() {
        GroupConfigManager mockGroupConfigManager = mock(GroupConfigManager.class);
        when(mockGroupConfigManager.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mockGroupConfigManager,
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertEquals(Optional.empty(), cache.shareGroupDlqTopicPrefix());
        verify(mockGroupConfigManager, times(1)).shareGroupDlqTopicPrefix();
    }

    // Tests for isDlqEnabledOnTopic

    @Test
    public void testIsDlqEnabledOnTopicReturnsFalseWhenTopicConfigNull() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        when(mockMetadataCache.topicConfig("test-topic")).thenReturn(null);

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertFalse(cache.isDlqEnabledOnTopic("test-topic"));
        verify(mockMetadataCache, times(1)).topicConfig("test-topic");
    }

    @Test
    public void testIsDlqEnabledOnTopicReturnsFalseWhenTopicConfigEmpty() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        when(mockMetadataCache.topicConfig("test-topic")).thenReturn(new Properties());

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertFalse(cache.isDlqEnabledOnTopic("test-topic"));
    }

    @Test
    public void testIsDlqEnabledOnTopicReturnsTrue() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        Properties props = new Properties();
        props.put(TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, "true");
        when(mockMetadataCache.topicConfig("test-topic")).thenReturn(props);

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertTrue(cache.isDlqEnabledOnTopic("test-topic"));
    }

    @Test
    public void testIsDlqEnabledOnTopicReturnsFalse() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        Properties props = new Properties();
        props.put(TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG, false);
        when(mockMetadataCache.topicConfig("test-topic")).thenReturn(props);

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertFalse(cache.isDlqEnabledOnTopic("test-topic"));
    }

    // Tests for dlqTopicMaxMessageBytes

    @Test
    public void testDlqTopicMaxMessageBytesFallsBackToBrokerConfigWhenTopicConfigNull() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        when(mockMetadataCache.topicConfig("test-topic")).thenReturn(null);

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> 5_000_000
        );

        assertEquals(5_000_000, cache.dlqTopicMaxMessageBytes("test-topic"));
    }

    @Test
    public void testDlqTopicMaxMessageBytesFallsBackToBrokerConfigWhenNoTopicOverride() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        when(mockMetadataCache.topicConfig("test-topic")).thenReturn(new Properties());

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> 5_000_000
        );

        assertEquals(5_000_000, cache.dlqTopicMaxMessageBytes("test-topic"));
    }

    @Test
    public void testDlqTopicMaxMessageBytesUsesTopicOverrideWhenPresent() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        Properties props = new Properties();
        props.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "10000000");
        when(mockMetadataCache.topicConfig("test-topic")).thenReturn(props);

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> 5_000_000
        );

        // Topic-level override wins even though it is larger than the broker-configured default.
        assertEquals(10_000_000, cache.dlqTopicMaxMessageBytes("test-topic"));
    }

    @Test
    public void testDlqTopicMaxMessageBytesFallsBackToBrokerConfigOnInvalidTopicOverride() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        Properties props = new Properties();
        props.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "not-a-number");
        when(mockMetadataCache.topicConfig("test-topic")).thenReturn(props);

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> 5_000_000
        );

        assertEquals(5_000_000, cache.dlqTopicMaxMessageBytes("test-topic"));
    }

    // Tests for topicName

    @Test
    public void testTopicNameReturnsNameWhenPresent() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        Uuid topicId = Uuid.randomUuid();
        when(mockMetadataCache.getTopicName(topicId)).thenReturn(Optional.of("some-topic"));

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertEquals(Optional.of("some-topic"), cache.topicName(topicId));
        verify(mockMetadataCache, times(1)).getTopicName(topicId);
    }

    @Test
    public void testTopicNameReturnsEmptyWhenNotPresent() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        Uuid topicId = Uuid.randomUuid();
        when(mockMetadataCache.getTopicName(topicId)).thenReturn(Optional.empty());

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertEquals(Optional.empty(), cache.topicName(topicId));
        verify(mockMetadataCache, times(1)).getTopicName(topicId);
    }

    @Test
    public void testTopicNameReturnsEmptyOnException() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        Uuid topicId = Uuid.randomUuid();
        when(mockMetadataCache.getTopicName(topicId)).thenThrow(new RuntimeException("boom"));

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mock(ListenerName.class),
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        assertEquals(Optional.empty(), cache.topicName(topicId));
        verify(mockMetadataCache, times(1)).getTopicName(topicId);
    }

    // Tests for topicPartitionData

    @Test
    public void testTopicPartitionDataReturnsFullData() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);
        Uuid topicId = Uuid.randomUuid();
        Node leader0 = new Node(0, "host0", 9092);
        Node leader1 = new Node(1, "host1", 9092);

        when(mockMetadataCache.getTopicId("test-topic")).thenReturn(topicId);
        when(mockMetadataCache.numPartitions("test-topic")).thenReturn(Optional.of(2));
        when(mockMetadataCache.getPartitionLeaderEndpoint("test-topic", 0, mockListenerName))
            .thenReturn(Optional.of(leader0));
        when(mockMetadataCache.getPartitionLeaderEndpoint("test-topic", 1, mockListenerName))
            .thenReturn(Optional.of(leader1));

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        ShareGroupDLQMetadataCacheHelper.TopicPartitionData data = cache.topicPartitionData("test-topic");

        assertEquals("test-topic", data.topicName());
        assertEquals(Optional.of(2), data.numPartitions());
        assertEquals(Optional.of(topicId), data.topicId());
        assertEquals(List.of(leader0, leader1), data.partitionLeaderNodes());

        verify(mockMetadataCache, times(1)).getTopicId("test-topic");
        verify(mockMetadataCache, times(1)).numPartitions("test-topic");
        verify(mockMetadataCache, times(1)).getPartitionLeaderEndpoint("test-topic", 0, mockListenerName);
        verify(mockMetadataCache, times(1)).getPartitionLeaderEndpoint("test-topic", 1, mockListenerName);
    }

    @Test
    public void testTopicPartitionDataReturnsEmptyTopicIdWhenZeroUuid() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);
        Node leader0 = new Node(0, "host0", 9092);

        when(mockMetadataCache.getTopicId("test-topic")).thenReturn(Uuid.ZERO_UUID);
        when(mockMetadataCache.numPartitions("test-topic")).thenReturn(Optional.of(1));
        when(mockMetadataCache.getPartitionLeaderEndpoint("test-topic", 0, mockListenerName))
            .thenReturn(Optional.of(leader0));

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        ShareGroupDLQMetadataCacheHelper.TopicPartitionData data = cache.topicPartitionData("test-topic");

        assertEquals("test-topic", data.topicName());
        assertEquals(Optional.of(1), data.numPartitions());
        assertEquals(Optional.empty(), data.topicId());
        assertEquals(List.of(leader0), data.partitionLeaderNodes());
    }

    @Test
    public void testTopicPartitionDataWithoutNumPartitions() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);
        Uuid topicId = Uuid.randomUuid();

        when(mockMetadataCache.getTopicId("test-topic")).thenReturn(topicId);
        when(mockMetadataCache.numPartitions("test-topic")).thenReturn(Optional.empty());

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        ShareGroupDLQMetadataCacheHelper.TopicPartitionData data = cache.topicPartitionData("test-topic");

        assertEquals("test-topic", data.topicName());
        assertEquals(Optional.empty(), data.numPartitions());
        assertEquals(Optional.of(topicId), data.topicId());
        assertEquals(List.of(), data.partitionLeaderNodes());

        verify(mockMetadataCache, times(1)).getTopicId("test-topic");
        verify(mockMetadataCache, times(1)).numPartitions("test-topic");
        verify(mockMetadataCache, times(0)).getPartitionLeaderEndpoint(any(), any(Integer.class), any());
    }

    @Test
    public void testTopicPartitionDataWithMissingPartitionLeader() {
        MetadataCache mockMetadataCache = mock(MetadataCache.class);
        ListenerName mockListenerName = mock(ListenerName.class);
        Uuid topicId = Uuid.randomUuid();
        Node leader0 = new Node(0, "host0", 9092);

        when(mockMetadataCache.getTopicId("test-topic")).thenReturn(topicId);
        when(mockMetadataCache.numPartitions("test-topic")).thenReturn(Optional.of(2));
        when(mockMetadataCache.getPartitionLeaderEndpoint("test-topic", 0, mockListenerName))
            .thenReturn(Optional.of(leader0));
        when(mockMetadataCache.getPartitionLeaderEndpoint("test-topic", 1, mockListenerName))
            .thenReturn(Optional.empty());

        ShareCoordinatorMetadataCacheHelperImpl cache = new ShareCoordinatorMetadataCacheHelperImpl(
            mockMetadataCache,
            sharePartitionKey -> 0,
            mockListenerName,
            mock(GroupConfigManager.class),
            () -> ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT
        );

        ShareGroupDLQMetadataCacheHelper.TopicPartitionData data = cache.topicPartitionData("test-topic");

        assertEquals("test-topic", data.topicName());
        assertEquals(Optional.of(2), data.numPartitions());
        assertEquals(Optional.of(topicId), data.topicId());
        assertEquals(Arrays.asList(leader0, null), data.partitionLeaderNodes());
    }
}
