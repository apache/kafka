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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.concurrent.ExecutionException;


/**
 * The result of {@link AdminClient#describeReplicaDir(Collection)}.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class DescribeReplicaDirResult {
    private final Map<TopicPartitionReplica, KafkaFuture<ReplicaDirInfo>> futures;

    DescribeReplicaDirResult(Map<TopicPartitionReplica, KafkaFuture<ReplicaDirInfo>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from replica to future which can be used to check the log directory information of individual replicas
     */
    public Map<TopicPartitionReplica, KafkaFuture<ReplicaDirInfo>> values() {
        return futures;
    }

    /**
     * Return a future which succeeds if log directory information of all replicas are available
     */
    public KafkaFuture<Map<TopicPartitionReplica, ReplicaDirInfo>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
            thenApply(new KafkaFuture.Function<Void, Map<TopicPartitionReplica, ReplicaDirInfo>>() {
                @Override
                public Map<TopicPartitionReplica, ReplicaDirInfo> apply(Void v) {
                    Map<TopicPartitionReplica, ReplicaDirInfo> replicaDirInfos = new HashMap<>();
                    for (Map.Entry<TopicPartitionReplica, KafkaFuture<ReplicaDirInfo>> entry : futures.entrySet()) {
                        try {
                            replicaDirInfos.put(entry.getKey(), entry.getValue().get());
                        } catch (InterruptedException | ExecutionException e) {
                            // This should be unreachable, because allOf ensured that all the futures
                            // completed successfully.
                            throw new RuntimeException(e);
                        }
                    }
                    return replicaDirInfos;
                }
            });
    }

    static public class ReplicaDirInfo {
        public String currentReplicaDir;
        public String temporaryReplicaDir;
        public long temporaryReplicaOffsetLag;

        public ReplicaDirInfo() {
            this(null, null, 0L);
        }

        public ReplicaDirInfo(String currentReplicaDir, String temporaryReplicaDir, long temporaryReplicaOffsetLag) {
            this.currentReplicaDir = currentReplicaDir;
            this.temporaryReplicaDir = temporaryReplicaDir;
            this.temporaryReplicaOffsetLag = temporaryReplicaOffsetLag;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (temporaryReplicaDir != null && !temporaryReplicaDir.isEmpty()) {
                builder.append("ReplicaDirInfo(currentReplicaDir=")
                    .append(currentReplicaDir)
                    .append(", temporaryReplicaDir=")
                    .append(temporaryReplicaDir)
                    .append(", temporaryReplicaOffsetLag=")
                    .append(temporaryReplicaOffsetLag)
                    .append(")");
            } else {
                builder.append("ReplicaDirInfo(currentReplicaDir=").append(currentReplicaDir).append(")");
            }
            return builder.toString();
        }
    }
}
