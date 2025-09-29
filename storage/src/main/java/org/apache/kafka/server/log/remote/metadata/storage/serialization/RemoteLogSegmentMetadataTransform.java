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
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class RemoteLogSegmentMetadataTransform implements RemoteLogMetadataTransform<RemoteLogSegmentMetadata> {

    public ApiMessageAndVersion toApiMessageAndVersion(RemoteLogSegmentMetadata segmentMetadata) {
        RemoteLogSegmentMetadataRecord record = new RemoteLogSegmentMetadataRecord()
                .setRemoteLogSegmentId(createRemoteLogSegmentIdEntry(segmentMetadata))
                .setStartOffset(segmentMetadata.startOffset())
                .setEndOffset(segmentMetadata.endOffset())
                .setBrokerId(segmentMetadata.brokerId())
                .setEventTimestampMs(segmentMetadata.eventTimestampMs())
                .setMaxTimestampMs(segmentMetadata.maxTimestampMs())
                .setSegmentSizeInBytes(segmentMetadata.segmentSizeInBytes())
                .setSegmentLeaderEpochs(createSegmentLeaderEpochsEntry(segmentMetadata))
                .setRemoteLogSegmentState(segmentMetadata.state().id())
                .setTxnIndexEmpty(segmentMetadata.isTxnIdxEmpty());
        segmentMetadata.customMetadata().ifPresent(md -> record.setCustomMetadata(md.value()));

        return new ApiMessageAndVersion(record, record.highestSupportedVersion());
    }

    private List<RemoteLogSegmentMetadataRecord.SegmentLeaderEpochEntry> createSegmentLeaderEpochsEntry(RemoteLogSegmentMetadata data) {
        return data.segmentLeaderEpochs().entrySet().stream()
                   .map(entry -> new RemoteLogSegmentMetadataRecord.SegmentLeaderEpochEntry()
                           .setLeaderEpoch(entry.getKey())
                           .setOffset(entry.getValue()))
                   .collect(Collectors.toList());
    }

    private RemoteLogSegmentMetadataRecord.RemoteLogSegmentIdEntry createRemoteLogSegmentIdEntry(RemoteLogSegmentMetadata data) {
        return new RemoteLogSegmentMetadataRecord.RemoteLogSegmentIdEntry()
                .setTopicIdPartition(
                        new RemoteLogSegmentMetadataRecord.TopicIdPartitionEntry()
                                .setId(data.remoteLogSegmentId().topicIdPartition().topicId())
                                .setName(data.remoteLogSegmentId().topicIdPartition().topic())
                                .setPartition(data.remoteLogSegmentId().topicIdPartition().partition()))
                .setId(data.remoteLogSegmentId().id());
    }

    @Override
    public RemoteLogSegmentMetadata fromApiMessageAndVersion(ApiMessageAndVersion apiMessageAndVersion) {
        RemoteLogSegmentMetadataRecord record = (RemoteLogSegmentMetadataRecord) apiMessageAndVersion.message();
        RemoteLogSegmentId remoteLogSegmentId = buildRemoteLogSegmentId(record.remoteLogSegmentId());

        Map<Integer, Long> segmentLeaderEpochs = new HashMap<>();
        for (RemoteLogSegmentMetadataRecord.SegmentLeaderEpochEntry segmentLeaderEpoch : record.segmentLeaderEpochs()) {
            segmentLeaderEpochs.put(segmentLeaderEpoch.leaderEpoch(), segmentLeaderEpoch.offset());
        }

        Optional<CustomMetadata> customMetadata = Optional.ofNullable(record.customMetadata()).map(CustomMetadata::new);
        RemoteLogSegmentMetadata remoteLogSegmentMetadata =
                new RemoteLogSegmentMetadata(remoteLogSegmentId, record.startOffset(), record.endOffset(),
                                             record.maxTimestampMs(), record.brokerId(),
                                             record.eventTimestampMs(), record.segmentSizeInBytes(),
                                             segmentLeaderEpochs, record.txnIndexEmpty());
        RemoteLogSegmentMetadataUpdate rlsmUpdate
                = new RemoteLogSegmentMetadataUpdate(remoteLogSegmentId, record.eventTimestampMs(),
                                                     customMetadata,
                                                     RemoteLogSegmentState.forId(record.remoteLogSegmentState()),
                                                     record.brokerId());

        return remoteLogSegmentMetadata.createWithUpdates(rlsmUpdate);
    }

    private RemoteLogSegmentId buildRemoteLogSegmentId(RemoteLogSegmentMetadataRecord.RemoteLogSegmentIdEntry entry) {
        TopicIdPartition topicIdPartition =
                new TopicIdPartition(entry.topicIdPartition().id(),
                                     new TopicPartition(entry.topicIdPartition().name(), entry.topicIdPartition().partition()));

        return new RemoteLogSegmentId(topicIdPartition, entry.id());
    }
}
