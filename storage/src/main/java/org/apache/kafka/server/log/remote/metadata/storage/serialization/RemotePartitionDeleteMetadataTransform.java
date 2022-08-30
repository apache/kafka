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
package org.apache.kafka.server.log.remote.metadata.storage.serialization;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemotePartitionDeleteMetadataRecord;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;

public final class RemotePartitionDeleteMetadataTransform implements RemoteLogMetadataTransform<RemotePartitionDeleteMetadata> {

    @Override
    public ApiMessageAndVersion toApiMessageAndVersion(RemotePartitionDeleteMetadata partitionDeleteMetadata) {
        RemotePartitionDeleteMetadataRecord record = new RemotePartitionDeleteMetadataRecord()
                .setTopicIdPartition(createTopicIdPartitionEntry(partitionDeleteMetadata.topicIdPartition()))
                .setEventTimestampMs(partitionDeleteMetadata.eventTimestampMs())
                .setBrokerId(partitionDeleteMetadata.brokerId())
                .setRemotePartitionDeleteState(partitionDeleteMetadata.state().id());
        return new ApiMessageAndVersion(record, record.highestSupportedVersion());
    }

    private RemotePartitionDeleteMetadataRecord.TopicIdPartitionEntry createTopicIdPartitionEntry(TopicIdPartition topicIdPartition) {
        return new RemotePartitionDeleteMetadataRecord.TopicIdPartitionEntry()
                .setName(topicIdPartition.topic())
                .setPartition(topicIdPartition.partition())
                .setId(topicIdPartition.topicId());
    }

    public RemotePartitionDeleteMetadata fromApiMessageAndVersion(ApiMessageAndVersion apiMessageAndVersion) {
        RemotePartitionDeleteMetadataRecord record = (RemotePartitionDeleteMetadataRecord) apiMessageAndVersion.message();
        TopicIdPartition topicIdPartition = new TopicIdPartition(record.topicIdPartition().id(),
                new TopicPartition(record.topicIdPartition().name(), record.topicIdPartition().partition()));

        return new RemotePartitionDeleteMetadata(topicIdPartition,
                RemotePartitionDeleteState.forId(record.remotePartitionDeleteState()),
                record.eventTimestampMs(), record.brokerId());
    }
}
