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

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RackAwareReplicaSelector implements ReplicaSelector {

    private final MostCaughtUpReplicaSelector tieBreaker = new MostCaughtUpReplicaSelector();

    @Override
    public Optional<ReplicaInfo> select(TopicPartition topicPartition,
                                        ClientMetadata clientMetadata,
                                        PartitionInfo partitionInfo) {
        if (clientMetadata.rackId != null && !clientMetadata.rackId.isEmpty()) {
            Set<ReplicaInfo> sameRackReplicas = partitionInfo.replicas().stream()
                    .filter(replicaInfo -> clientMetadata.rackId.equalsIgnoreCase(replicaInfo.endpoint().rack()))
                    .collect(Collectors.toSet());
            if (sameRackReplicas.isEmpty()) {
                return partitionInfo.leader();
            } else {
                Optional<ReplicaInfo> leader = partitionInfo.leader().filter(sameRackReplicas::contains);
                if (leader.isPresent()) {
                    // Use the leader if it's in this rack
                    return leader;
                } else {
                    // Otherwise, get the most caught-up replica
                    PartitionInfo sameRackPartition = new PartitionInfo() {
                        @Override
                        public Set<ReplicaInfo> replicas() {
                            return sameRackReplicas;
                        }

                        @Override
                        public Optional<ReplicaInfo> leader() {
                            return partitionInfo.leader();
                        }
                    };
                    return tieBreaker.select(topicPartition, clientMetadata, sameRackPartition);
                }
            }
        } else {
            return partitionInfo.leader();
        }
    }
}
