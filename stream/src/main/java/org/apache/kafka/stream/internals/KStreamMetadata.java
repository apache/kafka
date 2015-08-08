/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.stream.internals;

import org.apache.kafka.clients.processor.internals.PartitioningInfo;
import org.apache.kafka.clients.processor.internals.StreamGroup;

import java.util.Collections;
import java.util.Map;

public class KStreamMetadata {

    public static final String UNKNOWN_TOPICNAME = "__UNKNOWN_TOPIC__";
    public static final int UNKNOWN_PARTITION = -1;

    public static KStreamMetadata unjoinable() {
        return new KStreamMetadata(Collections.singletonMap(UNKNOWN_TOPICNAME, new PartitioningInfo(UNKNOWN_PARTITION)));
    }

    public StreamGroup streamGroup;
    public final Map<String, PartitioningInfo> topicPartitionInfos;

    public KStreamMetadata(Map<String, PartitioningInfo> topicPartitionInfos) {
        this.topicPartitionInfos = topicPartitionInfos;
    }

    boolean isJoinCompatibleWith(KStreamMetadata other) {
        // the two streams should only be joinable if they are inside the same sync group
        // and their contained streams all have the same number of partitions
        if (this.streamGroup != other.streamGroup)
            return false;

        int numPartitions = -1;
        for (PartitioningInfo partitionInfo : this.topicPartitionInfos.values()) {
            if (partitionInfo.numPartitions < 0) {
                return false;
            } else if (numPartitions >= 0) {
                if (partitionInfo.numPartitions != numPartitions)
                    return false;
            } else {
                numPartitions = partitionInfo.numPartitions;
            }
        }

        for (PartitioningInfo partitionInfo : other.topicPartitionInfos.values()) {
            if (partitionInfo.numPartitions != numPartitions)
                return false;
        }

        return true;
    }
}
