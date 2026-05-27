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

package org.apache.kafka.server.replica;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.quota.ReplicaQuota;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.internals.log.LogReadResult;

import java.util.List;

public interface ReplicaLogReader {
    /**
     * Util java record to encapsulate the input parameters
     *
     * @param topicIdPartition Represents the topicIdPartition to read data from.
     * @param partitionData    Represents the partition offset data.
     */
    record ReadPartitionInfo(TopicIdPartition topicIdPartition, FetchRequest.PartitionData partitionData) {
    }

    /**
     * Util java record to encapsulate the result of reading log from the topic partitions.
     *
     * @param topicIdPartition Represents the topicIdPartition for which data was read.
     * @param readResult       Represents the result corresponding to the topic partition.
     */
    record ReadResult(TopicIdPartition topicIdPartition, LogReadResult readResult) {
    }

    /**
     * Implementors must fetch record data from sources and return. At the time of writing
     * this method serves as the java equivalent for ReplicaManager.readFromLog. This helps
     * break dependency with core module.
     *
     * @param params            FetchParams encapsulate the replica details.
     * @param readPartitionInfo List of topic partitions and their offset into to read data from.
     * @param quota             ReplicaQuota representing any quote to be honored
     * @param readFromPurgatory Boolean representing whether data to be read from purgatory.
     * @return A list of ReadResult util record which represents the read results.
     */
    List<ReadResult> readFromLog(
        FetchParams params,
        List<ReadPartitionInfo> readPartitionInfo,
        ReplicaQuota quota,
        boolean readFromPurgatory
    );
}
