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
package org.apache.kafka.clients.admin;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The result of the {@link AdminClient#listOffsets(Map)} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListOffsetsResult {

    private final Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures;

    public ListOffsetsResult(Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures) {
        this.futures = futures;
    }

    /**
    * Return a future which can be used to check the result for a given partition.
    */
    public KafkaFuture<ListOffsetsResultInfo> partitionResult(final TopicPartition partition) {
        KafkaFuture<ListOffsetsResultInfo> future = futures.get(partition);
        if (future == null) {
            throw new IllegalArgumentException(
                    "List Offsets for partition \"" + partition + "\" was not attempted");
        }
        return future;
    }

    /**
     * Return a future which succeeds only if offsets for all specified partitions have been successfully
     * retrieved.
     */
    public KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]))
                .thenApply(new KafkaFuture.BaseFunction<Void, Map<TopicPartition, ListOffsetsResultInfo>>() {
                    @Override
                    public Map<TopicPartition, ListOffsetsResultInfo> apply(Void v) {
                        Map<TopicPartition, ListOffsetsResultInfo> offsets = new HashMap<>(futures.size());
                        for (Map.Entry<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> entry : futures.entrySet()) {
                            try {
                                offsets.put(entry.getKey(), entry.getValue().get());
                            } catch (InterruptedException | ExecutionException e) {
                                // This should be unreachable, because allOf ensured that all the futures completed successfully.
                                throw new RuntimeException(e);
                            }
                        }
                        return offsets;
                    }
                });
    }

    public static class ListOffsetsResultInfo {

        private final long offset;
        private final long timestamp;
        private final Optional<Integer> leaderEpoch;

        public ListOffsetsResultInfo(long offset, long timestamp, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.leaderEpoch = leaderEpoch;
        }

        public long offset() {
            return offset;
        }

        public long timestamp() {
            return timestamp;
        }

        public Optional<Integer> leaderEpoch() {
            return leaderEpoch;
        }

        @Override
        public String toString() {
            return "ListOffsetsResultInfo(offset=" + offset + ", timestamp=" + timestamp + ", leaderEpoch="
                    + leaderEpoch + ")";
        }
    }
}
