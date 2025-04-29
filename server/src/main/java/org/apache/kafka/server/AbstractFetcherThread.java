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

package org.apache.kafka.server;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class AbstractFetcherThread {

    public static class ReplicaFetch {
        private final Map<TopicPartition, FetchRequest.PartitionData> partitionData;
        private final FetchRequest.Builder fetchRequest;

        public ReplicaFetch(Map<TopicPartition, FetchRequest.PartitionData> partitionData,
                            FetchRequest.Builder fetchRequest) {
            this.partitionData = partitionData;
            this.fetchRequest = fetchRequest;
        }

        public Map<TopicPartition, FetchRequest.PartitionData> getPartitionData() {
            return partitionData;
        }

        public FetchRequest.Builder getFetchRequest() {
            return fetchRequest;
        }
    }

    public static class ResultWithPartitions<R> {
        private final R result;
        private final Set<TopicPartition> partitionsWithError;

        public ResultWithPartitions(R result, Set<TopicPartition> partitionsWithError) {
            this.result = result;
            this.partitionsWithError = partitionsWithError;
        }

        public R getResult() {
            return result;
        }

        public Set<TopicPartition> getPartitionsWithError() {
            return partitionsWithError;
        }
    }


    public interface ReplicaState { }

    public static final class Truncating implements ReplicaState {
        private static final Truncating INSTANCE = new Truncating();
        private Truncating() { }

        public static Truncating getInstance() {
            return INSTANCE;
        }

        @Override
        public String toString() {
            return "Truncating";
        }
    }

    public static final class Fetching implements ReplicaState {
        private static final Fetching INSTANCE = new Fetching();
        private Fetching() { }
        public static Fetching getInstance() {
            return INSTANCE;
        }

        @Override
        public String toString() {
            return "Fetching";
        }
    }

    /**
     * Class to keep partition offset and its state (truncatingLog, delayed)
     * This represents a partition as being either:
     * (1) Truncating its log, for example, having recently become a follower
     * (2) Delayed, for example, due to an error, where we subsequently back off a bit
     * (3) ReadyForFetch, the active state where the thread is actively fetching data.
     */
    public static class PartitionFetchState {
        private final Optional<Uuid> topicId;
        private final long fetchOffset;
        private final Optional<Long> lag;
        private final int currentLeaderEpoch;
        private final Optional<Long> delay;
        private final ReplicaState state;
        private final Optional<Integer> lastFetchedEpoch;
        private final Optional<Long> dueMs;


        public static PartitionFetchState create(Optional<Uuid> topicId,
                                                 long offset,
                                                 Optional<Long> lag,
                                                 int currentLeaderEpoch,
                                                 ReplicaState state,
                                                 Optional<Integer> lastFetchedEpoch) {
            return new PartitionFetchState(topicId, offset, lag, currentLeaderEpoch,
                    Optional.empty(), state, lastFetchedEpoch);
        }

        public PartitionFetchState(Optional<Uuid> topicId, long fetchOffset, Optional<Long> lag,
                                   int currentLeaderEpoch, Optional<Long> delay,
                                   ReplicaState state, Optional<Integer> lastFetchedEpoch) {
            this.topicId = topicId;
            this.fetchOffset = fetchOffset;
            this.lag = lag;
            this.currentLeaderEpoch = currentLeaderEpoch;
            this.delay = delay;
            this.state = state;
            this.lastFetchedEpoch = lastFetchedEpoch;

            // Initialize dueMs (equivalent to private val dueMs)
            this.dueMs = delay.isPresent() ?
                    Optional.of(delay.get() + Time.SYSTEM.milliseconds()) :
                    Optional.empty();
        }

        public boolean isReadyForFetch() {
            return state == Fetching.getInstance() && !isDelayed();
        }

        public boolean isReplicaInSync() {
            return lag.isPresent() && lag.get() <= 0;
        }

        public boolean isTruncating() {
            return state == Truncating.getInstance() && !isDelayed();
        }

        public boolean isDelayed() {
            return dueMs.isPresent() && dueMs.get() > Time.SYSTEM.milliseconds();
        }

        public Optional<Uuid> getTopicId() {
            return topicId;
        }

        public long getFetchOffset() {
            return fetchOffset;
        }

        public Optional<Long> getLag() {
            return lag;
        }

        public int getCurrentLeaderEpoch() {
            return currentLeaderEpoch;
        }

        public Optional<Long> getDelay() {
            return delay;
        }

        public ReplicaState getState() {
            return state;
        }

        public Optional<Integer> getLastFetchedEpoch() {
            return lastFetchedEpoch;
        }

        @Override
        public String toString() {
            return "FetchState(topicId=" + topicId +
                    ", fetchOffset=" + fetchOffset +
                    ", currentLeaderEpoch=" + currentLeaderEpoch +
                    ", lastFetchedEpoch=" + lastFetchedEpoch +
                    ", state=" + state +
                    ", lag=" + lag +
                    ", delay=" + (delay.isPresent() ? delay.get() : 0) + "ms)";
        }

        public PartitionFetchState updateTopicId(Optional<Uuid> newTopicId) {
            return new PartitionFetchState(newTopicId, this.fetchOffset, this.lag,
                    this.currentLeaderEpoch, this.delay,
                    this.state, this.lastFetchedEpoch);
        }
    }
}
