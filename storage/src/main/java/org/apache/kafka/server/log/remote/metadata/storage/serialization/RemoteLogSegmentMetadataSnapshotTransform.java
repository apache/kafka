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

import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogSegmentMetadataSnapshot;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataSnapshotRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class RemoteLogSegmentMetadataSnapshotTransform implements RemoteLogMetadataTransform<RemoteLogSegmentMetadataSnapshot> {

    public ApiMessageAndVersion toApiMessageAndVersion(RemoteLogSegmentMetadataSnapshot segmentMetadata) {
        RemoteLogSegmentMetadataSnapshotRecord record = new RemoteLogSegmentMetadataSnapshotRecord()
                .setSegmentId(segmentMetadata.segmentId())
                .setStartOffset(segmentMetadata.startOffset())
                .setEndOffset(segmentMetadata.endOffset())
                .setBrokerId(segmentMetadata.brokerId())
                .setEventTimestampMs(segmentMetadata.eventTimestampMs())
                .setMaxTimestampMs(segmentMetadata.maxTimestampMs())
                .setSegmentSizeInBytes(segmentMetadata.segmentSizeInBytes())
                .setSegmentLeaderEpochs(createSegmentLeaderEpochsEntry(segmentMetadata.segmentLeaderEpochs()))
                .setRemoteLogSegmentState(segmentMetadata.state().id());
        segmentMetadata.customMetadata().ifPresent(md -> record.setCustomMetadata(md.value()));

        return new ApiMessageAndVersion(record, record.highestSupportedVersion());
    }

    private List<RemoteLogSegmentMetadataSnapshotRecord.SegmentLeaderEpochEntry> createSegmentLeaderEpochsEntry(Map<Integer, Long> leaderEpochs) {
        return leaderEpochs.entrySet().stream()
                           .map(entry -> new RemoteLogSegmentMetadataSnapshotRecord.SegmentLeaderEpochEntry()
                           .setLeaderEpoch(entry.getKey())
                           .setOffset(entry.getValue()))
                           .collect(Collectors.toList());
    }

    @Override
    public RemoteLogSegmentMetadataSnapshot fromApiMessageAndVersion(ApiMessageAndVersion apiMessageAndVersion) {
        RemoteLogSegmentMetadataSnapshotRecord record = (RemoteLogSegmentMetadataSnapshotRecord) apiMessageAndVersion.message();
        Map<Integer, Long> segmentLeaderEpochs = new HashMap<>();
        for (RemoteLogSegmentMetadataSnapshotRecord.SegmentLeaderEpochEntry segmentLeaderEpoch : record.segmentLeaderEpochs()) {
            segmentLeaderEpochs.put(segmentLeaderEpoch.leaderEpoch(), segmentLeaderEpoch.offset());
        }

        Optional<CustomMetadata> customMetadata = Optional.ofNullable(record.customMetadata()).map(CustomMetadata::new);
        return new RemoteLogSegmentMetadataSnapshot(record.segmentId(),
                                                    record.startOffset(),
                                                    record.endOffset(),
                                                    record.maxTimestampMs(),
                                                    record.brokerId(),
                                                    record.eventTimestampMs(),
                                                    record.segmentSizeInBytes(),
                                                    customMetadata,
                                                    RemoteLogSegmentState.forId(record.remoteLogSegmentState()),
                                                    segmentLeaderEpochs);
    }

}
