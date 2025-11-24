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
package org.apache.kafka.server.share.session;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.ShareGroupListener;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Iterator;
import java.util.List;

import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.clearYammerMetrics;
import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.yammerMetricValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ShareSessionCacheTest {

    @BeforeEach
    public void setUp() {
        clearYammerMetrics();
    }

    @Test
    public void testShareSessionCache() throws InterruptedException {
        ShareSessionCache cache = new ShareSessionCache(3);
        assertEquals(0, cache.size());
        ShareSessionKey key1 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(10), "conn-1");
        ShareSessionKey key2 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(20), "conn-2");
        ShareSessionKey key3 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(30), "conn-3");
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(40), "conn-4"));
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(5), "conn-5"));
        assertShareCacheContains(cache, List.of(key1, key2, key3));

        assertMetricsValues(3, 60, 0, cache);
    }

    @Test
    public void testResizeCachedSessions() throws InterruptedException {
        ShareSessionCache cache = new ShareSessionCache(2);
        assertEquals(0, cache.size());
        assertEquals(0, cache.totalPartitions());
        ShareSessionKey key1 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(2), "conn-1");
        assertNotNull(key1);
        assertShareCacheContains(cache, List.of(key1));
        ShareSession session1 = cache.get(key1);
        assertEquals(2, session1.size());
        assertEquals(2, cache.totalPartitions());
        assertEquals(1, cache.size());

        assertMetricsValues(1, 2, 0, cache);

        ShareSessionKey key2 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(4), "conn-2");
        assertNotNull(key2);
        assertShareCacheContains(cache, List.of(key1, key2));
        ShareSession session2 = cache.get(key2);
        assertEquals(6, cache.totalPartitions());
        assertEquals(2, cache.size());
        cache.updateNumPartitions(session1);
        cache.updateNumPartitions(session2);

        assertMetricsValues(2, 6, 0, cache);

        ShareSessionKey key3 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(5), "conn-3");
        assertNull(key3);
        assertShareCacheContains(cache, List.of(key1, key2));
        assertEquals(6, cache.totalPartitions());
        assertEquals(2, cache.size());
        cache.remove(key1);
        assertShareCacheContains(cache, List.of(key2));
        assertEquals(1, cache.size());
        assertEquals(4, cache.totalPartitions());

        assertMetricsValues(1, 4, 0, cache);

        Iterator<CachedSharePartition> iterator = session2.partitionMap().iterator();
        iterator.next();
        iterator.remove();
        // Session size should get updated as it's backed by the partition map.
        assertEquals(3, session2.size());
        // Cached size should not get updated as it shall update on touch.
        assertEquals(4, session2.cachedSize());
        assertEquals(4, cache.totalPartitions());
        // Touch the session to update the changes in cache and session's cached size.
        cache.updateNumPartitions(session2);
        assertEquals(3, session2.cachedSize());
        assertEquals(3, cache.totalPartitions());

        assertMetricsValues(1, 3, 0, cache);
    }

    @Test
    public void testRemoveConnection() throws InterruptedException {
        ShareSessionCache cache = new ShareSessionCache(3);
        assertEquals(0, cache.size());
        ShareSessionKey key1 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(1), "conn-1");
        ShareSessionKey key2 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(2), "conn-2");
        ShareSessionKey key3 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(3), "conn-3");

        assertMetricsValues(3, 6, 0, cache);

        // Since cache size is now equal to max entries allowed(3), no new session can be created.
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(40), "conn-4"));
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(5), "conn-5"));
        assertShareCacheContains(cache, List.of(key1, key2, key3));

        assertMetricsValues(3, 6, 0, cache);

        // Simulating the disconnection of client with connection id conn-1
        cache.connectionDisconnectListener().onDisconnect("conn-1");
        assertShareCacheContains(cache, List.of(key2, key3));

        assertMetricsValues(2, 5, 1, cache);

        // Since one client got disconnected, we can add another one now
        ShareSessionKey key4 = cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(4), "conn-6");
        assertShareCacheContains(cache, List.of(key2, key3, key4));

        assertMetricsValues(3, 9, 1, cache);
    }

    @Test
    public void testRemoveAllSessions() {
        ShareSessionCache cache = new ShareSessionCache(3);
        assertEquals(0, cache.size());
        assertEquals(0, cache.totalPartitions());
        cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(10), "conn-1");
        cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(20), "conn-2");
        cache.maybeCreateSession("grp", Uuid.randomUuid().toString(), mockedSharePartitionMap(30), "conn-3");
        assertEquals(3, cache.size());
        assertEquals(60, cache.totalPartitions());
        cache.removeAllSessions();
        assertEquals(0, cache.size());
        assertEquals(0, cache.totalPartitions());
    }

    @Test
    public void testShareGroupListenerEvents() {
        ShareGroupListener mockListener = Mockito.mock(ShareGroupListener.class);
        ShareSessionCache cache = new ShareSessionCache(3);
        cache.registerShareGroupListener(mockListener);

        String groupId = "grp";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        ShareSessionKey key1 = cache.maybeCreateSession(groupId, memberId1, mockedSharePartitionMap(1), "conn-1");
        ShareSessionKey key2 = cache.maybeCreateSession(groupId, memberId2, mockedSharePartitionMap(1), "conn-2");

        // Verify member count is tracked
        assertEquals(2, cache.size());
        assertNotNull(cache.get(key1));
        assertNotNull(cache.get(key2));
        assertEquals(2, cache.numMembers(groupId));

        // Remove session and verify listener are not called as connection disconnect listener didn't
        // remove the session.
        cache.remove(key1);
        Mockito.verify(mockListener, Mockito.times(0)).onMemberLeave(groupId, memberId1);
        Mockito.verify(mockListener, Mockito.times(0)).onGroupEmpty(groupId);
        // Verify member count is updated
        assertEquals(1, cache.numMembers(groupId));

        // Re-create session for memberId1.
        cache.maybeCreateSession(groupId, memberId1, mockedSharePartitionMap(1), "conn-1");
        assertEquals(2, cache.numMembers(groupId));

        // Simulate connection disconnect for memberId1.
        cache.connectionDisconnectListener().onDisconnect("conn-1");
        // Verify only member leave event is triggered for memberId1. Empty group event should not be triggered.
        Mockito.verify(mockListener, Mockito.times(1)).onMemberLeave(groupId, memberId1);
        Mockito.verify(mockListener, Mockito.times(0)).onMemberLeave(groupId, memberId2);
        Mockito.verify(mockListener, Mockito.times(0)).onGroupEmpty(groupId);
        assertEquals(1, cache.numMembers(groupId));

        // Simulate connection disconnect for memberId2.
        cache.connectionDisconnectListener().onDisconnect("conn-2");
        // Verify both member leave event and empty group event should be triggered.
        Mockito.verify(mockListener, Mockito.times(1)).onMemberLeave(groupId, memberId1);
        Mockito.verify(mockListener, Mockito.times(1)).onMemberLeave(groupId, memberId2);
        Mockito.verify(mockListener, Mockito.times(1)).onGroupEmpty(groupId);
        assertNull(cache.numMembers(groupId));
    }

    @Test
    public void testShareGroupListenerEventsMultipleGroups() {
        ShareGroupListener mockListener = Mockito.mock(ShareGroupListener.class);
        ShareSessionCache cache = new ShareSessionCache(3);
        cache.registerShareGroupListener(mockListener);

        String groupId1 = "grp1";
        String groupId2 = "grp2";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        ShareSessionKey key1 = cache.maybeCreateSession(groupId1, memberId1, mockedSharePartitionMap(1), "conn-1");
        ShareSessionKey key2 = cache.maybeCreateSession(groupId2, memberId2, mockedSharePartitionMap(1), "conn-2");

        // Verify member count is tracked
        assertEquals(2, cache.size());
        assertNotNull(cache.get(key1));
        assertNotNull(cache.get(key2));
        assertEquals(1, cache.numMembers(groupId1));
        assertEquals(1, cache.numMembers(groupId2));

        // Remove session for group1 and verify listeners are only called for group1.
        cache.connectionDisconnectListener().onDisconnect("conn-1");
        Mockito.verify(mockListener, Mockito.times(1)).onMemberLeave(groupId1, memberId1);
        Mockito.verify(mockListener, Mockito.times(1)).onGroupEmpty(groupId1);
        // Listener should not be called for group2.
        Mockito.verify(mockListener, Mockito.times(0)).onMemberLeave(groupId2, memberId2);
        Mockito.verify(mockListener, Mockito.times(0)).onGroupEmpty(groupId2);
        // Verify member count is updated.
        assertNull(cache.numMembers(groupId1));
        assertEquals(1, cache.numMembers(groupId2));
    }

    @Test
    public void testNoShareGroupListenerRegistered() {
        ShareSessionCache cache = new ShareSessionCache(3);

        String groupId = "grp";
        ShareSessionKey key = cache.maybeCreateSession(groupId, Uuid.randomUuid().toString(), mockedSharePartitionMap(1), "conn-1");

        // Verify member count is still tracked even without listener
        assertEquals(1, cache.numMembers(groupId));
        assertNotNull(cache.get(key));

        // Remove session should not throw any exceptions.
        cache.connectionDisconnectListener().onDisconnect("conn-1");
        assertNull(cache.numMembers(groupId));
    }

    private ImplicitLinkedHashCollection<CachedSharePartition> mockedSharePartitionMap(int size) {
        ImplicitLinkedHashCollection<CachedSharePartition> cacheMap = new
                ImplicitLinkedHashCollection<>(size);
        for (int i = 0; i < size; i++)
            cacheMap.add(new CachedSharePartition("test", Uuid.randomUuid(), i, false));
        return cacheMap;
    }

    private void assertShareCacheContains(ShareSessionCache cache,
                                         List<ShareSessionKey> sessionKeys) {
        int i = 0;
        assertEquals(sessionKeys.size(), cache.size());
        for (ShareSessionKey sessionKey : sessionKeys) {
            assertFalse(cache.get(sessionKey).isEmpty(),
                    "Missing session " + ++i + " out of " + sessionKeys.size() + " ( " + sessionKey + " )");
        }
    }

    private void assertMetricsValues(
        int shareSessionsCount,
        int sharePartitionsCount,
        int evictionsCount,
        ShareSessionCache cache
    ) throws InterruptedException {
        TestUtils.waitForCondition(() -> yammerMetricValue(ShareSessionCache.SHARE_SESSIONS_COUNT).intValue() == shareSessionsCount,
            "Share session count should be " + shareSessionsCount);
        TestUtils.waitForCondition(() -> yammerMetricValue(ShareSessionCache.SHARE_PARTITIONS_COUNT).intValue() == sharePartitionsCount,
            "Share partition count should be " + sharePartitionsCount);
        assertEquals(evictionsCount, cache.evictionsMeter().count());
    }
}
