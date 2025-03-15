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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.common.TopicIdPartition;

/**
 * @param topicIdPartition The topic ID and partition index of the replica.
 * @param directoryId      The ID of the directory we are placing the replica into.
 * @param submissionTimeNs The time in monotonic nanosecond when this assignment was created.
 * @param successCallback  The callback to invoke on success.
 */
record Assignment(
    TopicIdPartition topicIdPartition,
    Uuid directoryId,
    long submissionTimeNs,
    Runnable successCallback
) {

    /**
     * Check if this Assignment is still valid to be sent.
     *
     * @param nodeId The broker ID.
     * @param image  The metadata image.
     * @return True only if the Assignment is still valid.
     */
    boolean valid(int nodeId, MetadataImage image) {
        TopicImage topicImage = image.topics().getTopic(topicIdPartition.topicId());
        if (topicImage == null) {
            return false; // The topic has been deleted.
        }
        PartitionRegistration partition = topicImage.partitions().get(topicIdPartition.partitionId());
        if (partition == null) {
            return false; // The partition no longer exists.
        }
        // Check if this broker is still a replica.
        return Replicas.contains(partition.replicas, nodeId);
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("Assignment");
        bld.append("(topicIdPartition=").append(topicIdPartition);
        bld.append(", directoryId=").append(directoryId);
        bld.append(", submissionTimeNs=").append(submissionTimeNs);
        bld.append(", successCallback=").append(successCallback);
        bld.append(")");
        return bld.toString();
    }
}
