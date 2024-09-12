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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ShareSessionCacheTest {

    @Test
    public void testShareSessionCache() {
        ShareSessionCache cache = new ShareSessionCache(3, 100);
        assertEquals(0, cache.size());
        ShareSessionKey key1 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 0, mockedSharePartitionMap(10));
        ShareSessionKey key2 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 10, mockedSharePartitionMap(20));
        ShareSessionKey key3 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 20, mockedSharePartitionMap(30));
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid(), 30, mockedSharePartitionMap(40)));
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid(), 40, mockedSharePartitionMap(5)));
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key2, key3)));
        cache.touch(cache.get(key1), 200);
        ShareSessionKey key4 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 210, mockedSharePartitionMap(11));
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key3, key4)));
        cache.touch(cache.get(key1), 400);
        cache.touch(cache.get(key3), 390);
        cache.touch(cache.get(key4), 400);
        ShareSessionKey key5 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 410, mockedSharePartitionMap(50));
        assertNull(key5);
    }

    @Test
    public void testResizeCachedSessions() {
        ShareSessionCache cache = new ShareSessionCache(2, 100);
        assertEquals(0, cache.size());
        assertEquals(0, cache.totalPartitions());
        ShareSessionKey key1 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 0, mockedSharePartitionMap(2));
        assertNotNull(key1);
        assertShareCacheContains(cache, new ArrayList<>(Collections.singletonList(key1)));
        ShareSession session1 = cache.get(key1);
        assertEquals(2, session1.size());
        assertEquals(2, cache.totalPartitions());
        assertEquals(1, cache.size());

        ShareSessionKey key2 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 0, mockedSharePartitionMap(4));
        assertNotNull(key2);
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key2)));
        ShareSession session2 = cache.get(key2);
        assertEquals(6, cache.totalPartitions());
        assertEquals(2, cache.size());
        cache.touch(session1, 200);
        cache.touch(session2, 200);

        ShareSessionKey key3 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 200, mockedSharePartitionMap(5));
        assertNull(key3);
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key2)));
        assertEquals(6, cache.totalPartitions());
        assertEquals(2, cache.size());
        cache.remove(key1);
        assertShareCacheContains(cache, new ArrayList<>(Collections.singletonList(key2)));
        assertEquals(1, cache.size());
        assertEquals(4, cache.totalPartitions());

        Iterator<CachedSharePartition> iterator = session2.partitionMap().iterator();
        iterator.next();
        iterator.remove();
        assertEquals(3, session2.size());
        assertEquals(4, session2.cachedSize());
        cache.touch(session2, session2.lastUsedMs());
        assertEquals(3, cache.totalPartitions());
    }

    private ImplicitLinkedHashCollection<CachedSharePartition> mockedSharePartitionMap(int size) {
        ImplicitLinkedHashCollection<CachedSharePartition> cacheMap = new
                ImplicitLinkedHashCollection<>(size);
        for (int i = 0; i < size; i++)
            cacheMap.add(new CachedSharePartition("test", Uuid.randomUuid(), i, false));
        return cacheMap;
    }

    private void assertShareCacheContains(ShareSessionCache cache,
                                         ArrayList<ShareSessionKey> sessionKeys) {
        int i = 0;
        assertEquals(sessionKeys.size(), cache.size());
        for (ShareSessionKey sessionKey : sessionKeys) {
            assertFalse(cache.get(sessionKey).isEmpty(),
                    "Missing session " + ++i + " out of " + sessionKeys.size() + " ( " + sessionKey + " )");
        }
    }
}
