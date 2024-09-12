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
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataUpdateRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import java.util.Optional;

public class RemoteLogSegmentMetadataUpdateTransform implements RemoteLogMetadataTransform<RemoteLogSegmentMetadataUpdate> {

    public ApiMessageAndVersion toApiMessageAndVersion(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate) {
        RemoteLogSegmentMetadataUpdateRecord record = new RemoteLogSegmentMetadataUpdateRecord()
                .setRemoteLogSegmentId(createRemoteLogSegmentIdEntry(segmentMetadataUpdate))
                .setBrokerId(segmentMetadataUpdate.brokerId())
                .setEventTimestampMs(segmentMetadataUpdate.eventTimestampMs())
                .setRemoteLogSegmentState(segmentMetadataUpdate.state().id());
        segmentMetadataUpdate.customMetadata().ifPresent(md -> record.setCustomMetadata(md.value()));

        return new ApiMessageAndVersion(record, record.highestSupportedVersion());
    }

    public RemoteLogSegmentMetadataUpdate fromApiMessageAndVersion(ApiMessageAndVersion apiMessageAndVersion) {
        RemoteLogSegmentMetadataUpdateRecord record = (RemoteLogSegmentMetadataUpdateRecord) apiMessageAndVersion.message();
        RemoteLogSegmentMetadataUpdateRecord.RemoteLogSegmentIdEntry entry = record.remoteLogSegmentId();
        TopicIdPartition topicIdPartition = new TopicIdPartition(entry.topicIdPartition().id(),
                new TopicPartition(entry.topicIdPartition().name(), entry.topicIdPartition().partition()));

        Optional<CustomMetadata> customMetadata = Optional.ofNullable(record.customMetadata()).map(CustomMetadata::new);
        return new RemoteLogSegmentMetadataUpdate(new RemoteLogSegmentId(topicIdPartition, entry.id()),
                record.eventTimestampMs(), customMetadata, RemoteLogSegmentState.forId(record.remoteLogSegmentState()), record.brokerId());
    }

    private RemoteLogSegmentMetadataUpdateRecord.RemoteLogSegmentIdEntry createRemoteLogSegmentIdEntry(RemoteLogSegmentMetadataUpdate data) {
        return new RemoteLogSegmentMetadataUpdateRecord.RemoteLogSegmentIdEntry()
                .setId(data.remoteLogSegmentId().id())
                .setTopicIdPartition(
                        new RemoteLogSegmentMetadataUpdateRecord.TopicIdPartitionEntry()
                                .setName(data.remoteLogSegmentId().topicIdPartition().topic())
                                .setPartition(data.remoteLogSegmentId().topicIdPartition().partition())
                                .setId(data.remoteLogSegmentId().topicIdPartition().topicId()));
    }

}
