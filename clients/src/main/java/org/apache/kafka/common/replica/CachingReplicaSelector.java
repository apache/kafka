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
package org.apache.kafka.common.replica;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class CachingReplicaSelector implements ReplicaSelector {
    public static final long CACHE_VALID_MS = 10000;

    private final Time time;

    private final ReplicaSelector delegate;

    private final Map<TopicPartition, Optional<CachedReplicaInfo>> cachedResults;

    public CachingReplicaSelector(Time time, ReplicaSelector delegate) {
        this.time = time;
        this.delegate = delegate;
        this.cachedResults = new HashMap<>();
    }

    @Override
    public Optional<ReplicaView> select(TopicPartition topicPartition,
                                        ClientMetadata clientMetadata,
                                        PartitionView partitionView) {
        Optional<CachedReplicaInfo> cachedReplicaInfo = cachedResults.compute(topicPartition, (key, existingValue) -> {
            long now = time.milliseconds();
            // Recompute if the value is null, empty, or expired
            if (existingValue == null || !existingValue.isPresent() || existingValue.get().expired(now)) {
                Optional<ReplicaView> replicaInfo = delegate.select(key, clientMetadata, partitionView);
                return replicaInfo.map(info -> new CachedReplicaInfo(info, now + CACHE_VALID_MS));
            } else {
                return existingValue;
            }
        });

        return cachedReplicaInfo.map(CachedReplicaInfo::replicaInfo);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        delegate.configure(configs);
    }

    static class CachedReplicaInfo {
        private final ReplicaView replicaView;
        private final long expireTimeMs;

        CachedReplicaInfo(ReplicaView replicaView, long expireTimeMs) {
            this.replicaView = replicaView;
            this.expireTimeMs = expireTimeMs;
        }

        ReplicaView replicaInfo() {
            return replicaView;
        }

        boolean expired(long now) {
            return now > expireTimeMs;
        }
    }
}
