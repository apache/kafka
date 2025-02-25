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

import kafka.server.MetadataCache;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.persister.ShareCoordinatorMetadataCacheHelper;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

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
            mock(ListenerName.class)
        ));
        assertEquals("metadataCache must not be null", e.getMessage());

        e = assertThrows(NullPointerException.class, () -> new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            null,
            mock(ListenerName.class)
        ));
        assertEquals("keyToPartitionMapper must not be null", e.getMessage());

        e = assertThrows(NullPointerException.class, () -> new ShareCoordinatorMetadataCacheHelperImpl(
            mock(MetadataCache.class),
            func,
            null
        ));
        assertEquals("interBrokerListenerName must not be null", e.getMessage());
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
            mock(ListenerName.class)
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
            mock(ListenerName.class)
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
            mock(ListenerName.class)
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
            mockListenerName
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
            CollectionConverters.asScala(List.of())
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
            CollectionConverters.asScala(List.of(
                new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode(Errors.BROKER_NOT_AVAILABLE.code())
            ))
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
            mockListenerName
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
            CollectionConverters.asScala(List.of(
                new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode(Errors.NONE.code())
                    .setPartitions(List.of(
                        new MetadataResponseData.MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setLeaderId(1)
                    ))
            ))
        );

        // get alive broker node throws exception
        when(mockMetadataCache.getAliveBrokerNode(
            eq(1),
            eq(mockListenerName)
        )).thenReturn(
            OptionConverters.toScala(Optional.empty())
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
            mockListenerName
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
            CollectionConverters.asScala(List.of(
                new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode(Errors.NONE.code())
                    .setPartitions(List.of(
                        new MetadataResponseData.MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setLeaderId(1)
                    ))
            ))
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
            mockListenerName
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
            CollectionConverters.asScala(List.of(
                new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode(Errors.NONE.code())
                    .setPartitions(List.of(
                        new MetadataResponseData.MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setLeaderId(1)
                    ))
            ))
        );

        // get alive broker node throws exception
        Node node = new Node(2, "some.domain.name", 65534);
        when(mockMetadataCache.getAliveBrokerNode(
            eq(1),
            eq(mockListenerName)
        )).thenReturn(
            OptionConverters.toScala(Optional.of(node))
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
            mockListenerName
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
            mockListenerName
        );

        List<Node> nodes = List.of(
            new Node(0, "some.domain.name", 65534),
            new Node(1, "some.domain.name", 12345)
        );

        when(mockMetadataCache.getAliveBrokerNodes(
            eq(mockListenerName)
        )).thenReturn(
            CollectionConverters.asScala(
                nodes
            )
        );

        assertEquals(
            nodes,
            cache.getClusterNodes()
        );

        verify(mockMetadataCache, times(1)).getAliveBrokerNodes(eq(mockListenerName));
    }
}
