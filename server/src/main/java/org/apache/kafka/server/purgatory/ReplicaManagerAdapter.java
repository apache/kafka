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
package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.server.log.remote.TopicPartitionLog;
import org.apache.kafka.server.quota.ReplicaQuota;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.util.LinkedHashMap;

/**
 * Interface to decouple {@link DelayedFetch} from ReplicaManager.
 */
public interface ReplicaManagerAdapter {

    /**
     * Whether the leader should throttle the given replica for the partition.
     * To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
     * the quota is exceeded and the replica is not in sync.
     *
     * @param quota The replica quota
     * @param partition The topic partition log
     * @param replicaId The replica ID
     * @return Whether the leader should throttle the replica
     */
    static boolean shouldLeaderThrottle(ReplicaQuota quota, TopicPartitionLog partition, int replicaId) {
        return !partition.isReplicaInSync(replicaId) && quota.isThrottled(partition.topicPartition()) && quota.isQuotaExceeded();
    }

    /**
     * Get the partition for the provided topic partition.
     *
     * @param topicPartition The topic partition
     * @return The partition log
     */
    TopicPartitionLog getPartitionOrException(TopicPartition topicPartition);

    /**
     * Read from the log when completing delayed fetches from purgatory.
     *
     * @param params The fetch parameters
     * @param readPartitionInfo The partitions to read
     * @param quota The replica quota
     * @return The fetch partition data for each partition
     */
    LinkedHashMap<TopicIdPartition, FetchPartitionData> readFromLogByPurgatory(
        FetchParams params,
        LinkedHashMap<TopicIdPartition, PartitionData> readPartitionInfo,
        ReplicaQuota quota
    );
}
