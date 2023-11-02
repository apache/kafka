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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class RemoteLogMetadataTopicPartitioner {
    public static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataTopicPartitioner.class);
    private final int numMetadataTopicPartitions;

    public RemoteLogMetadataTopicPartitioner(int numMetadataTopicPartitions) {
        this.numMetadataTopicPartitions = numMetadataTopicPartitions;
    }

    public int metadataPartition(TopicIdPartition topicIdPartition) {
        Objects.requireNonNull(topicIdPartition, "TopicPartition can not be null");

        int partitionNum = Utils.toPositive(Utils.murmur2(toBytes(topicIdPartition))) % numMetadataTopicPartitions;
        log.debug("No of partitions [{}], partitionNum: [{}] for given topic: [{}]", numMetadataTopicPartitions, partitionNum, topicIdPartition);
        return partitionNum;
    }

    private byte[] toBytes(TopicIdPartition topicIdPartition) {
        // We do not want to depend upon hash code generation of Uuid as that may change.
        int hash = Objects.hash(topicIdPartition.topicId().getLeastSignificantBits(),
                                topicIdPartition.topicId().getMostSignificantBits(),
                                topicIdPartition.partition());

        return toBytes(hash);
    }

    private byte[] toBytes(int n) {
        return new byte[]{
            (byte) (n >> 24),
            (byte) (n >> 16),
            (byte) (n >> 8),
            (byte) n
        };
    }
}
