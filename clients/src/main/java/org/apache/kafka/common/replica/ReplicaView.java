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

import org.apache.kafka.common.Node;

import java.util.Comparator;
import java.util.Optional;

/**
 * View of a replica used by {@link ReplicaSelector} to determine a preferred replica.
 */
public interface ReplicaView {
    /**
     * Is this replica the leader of its partition
     */
    boolean isLeader();

    /**
     * The endpoint information for this replica (hostname, port, rack, etc)
     */
    Node endpoint();

    /**
     * The log end offset for this replica
     */
    long logEndOffset();

    /**
     * The number of milliseconds (if any) since the last time this replica was caught up to the high watermark.
     */
    Optional<Long> lastCaughtUpTimeMs();

    /**
     * Comparator for ReplicaView that returns in the order of "most caught up". This is used for deterministic
     * selection of a replica when there is a tie from a selector.
     */
    static Comparator<ReplicaView> comparator() {
        return Comparator.comparing(ReplicaView::logEndOffset)
            .thenComparing(replicaView -> replicaView.lastCaughtUpTimeMs().orElse(-1L))
            .thenComparing(replicaInfo -> replicaInfo.endpoint().id());
    }

    class DefaultReplicaView implements ReplicaView {
        private final boolean isLeader;
        private final Node endpoint;
        private final long logEndOffset;
        private final Optional<Long> lastCaughtUpTimeMs;

        public DefaultReplicaView(boolean isLeader, Node endpoint, long logEndOffset, Optional<Long> lastCaughtUpTimeMs) {
            this.isLeader = isLeader;
            this.endpoint = endpoint;
            this.logEndOffset = logEndOffset;
            this.lastCaughtUpTimeMs = lastCaughtUpTimeMs;
        }

        @Override
        public boolean isLeader() {
            return isLeader;
        }

        @Override
        public Node endpoint() {
            return endpoint;
        }

        @Override
        public long logEndOffset() {
            return logEndOffset;
        }

        @Override
        public Optional<Long> lastCaughtUpTimeMs() {
            return lastCaughtUpTimeMs;
        }
    }
}
