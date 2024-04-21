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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.serialization.BytesApiMessageSerde;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogSegmentMetadataSnapshot;
import org.apache.kafka.server.log.remote.metadata.storage.generated.MetadataRecordType;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataSnapshotRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataUpdateRecord;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemotePartitionDeleteMetadataRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;

import java.io.PrintStream;

/**
 * This class provides serialization and deserialization for {@link RemoteLogMetadata}. This is the root serde
 * for the messages that are stored in internal remote log metadata topic.
 */
public class RemoteLogMetadataSerde {
    private static final short REMOTE_LOG_SEGMENT_METADATA_API_KEY = new RemoteLogSegmentMetadataRecord().apiKey();
    private static final short REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY = new RemoteLogSegmentMetadataUpdateRecord().apiKey();
    private static final short REMOTE_PARTITION_DELETE_API_KEY = new RemotePartitionDeleteMetadataRecord().apiKey();
    private static final short REMOTE_LOG_SEGMENT_METADATA_SNAPSHOT_API_KEY = new RemoteLogSegmentMetadataSnapshotRecord().apiKey();

    private final BytesApiMessageSerde bytesApiMessageSerde;

    private final RemoteLogSegmentMetadataTransform segmentTransform;
    private final RemoteLogSegmentMetadataUpdateTransform segmentUpdateTransform;
    private final RemotePartitionDeleteMetadataTransform partitionDeleteTransform;
    private final RemoteLogSegmentMetadataSnapshotTransform segmentSnapshotTransform;

    public RemoteLogMetadataSerde() {
        bytesApiMessageSerde = new BytesApiMessageSerde() {
            @Override
            public ApiMessage apiMessageFor(short apiKey) {
                return newApiMessage(apiKey);
            }
        };
        segmentTransform = new RemoteLogSegmentMetadataTransform();
        segmentUpdateTransform = new RemoteLogSegmentMetadataUpdateTransform();
        partitionDeleteTransform = new RemotePartitionDeleteMetadataTransform();
        segmentSnapshotTransform = new RemoteLogSegmentMetadataSnapshotTransform();
    }

    protected ApiMessage newApiMessage(short apiKey) {
        return MetadataRecordType.fromId(apiKey).newMetadataRecord();
    }

    public byte[] serialize(RemoteLogMetadata remoteLogMetadata) {

        ApiMessageAndVersion apiMessageAndVersion;
        if (remoteLogMetadata instanceof RemoteLogSegmentMetadata) {
            apiMessageAndVersion = segmentTransform.toApiMessageAndVersion((RemoteLogSegmentMetadata) remoteLogMetadata);
        } else if (remoteLogMetadata instanceof RemoteLogSegmentMetadataUpdate) {
            apiMessageAndVersion = segmentUpdateTransform.toApiMessageAndVersion((RemoteLogSegmentMetadataUpdate) remoteLogMetadata);
        } else if (remoteLogMetadata instanceof RemotePartitionDeleteMetadata) {
            apiMessageAndVersion = partitionDeleteTransform.toApiMessageAndVersion((RemotePartitionDeleteMetadata) remoteLogMetadata);
        } else if (remoteLogMetadata instanceof RemoteLogSegmentMetadataSnapshot) {
            apiMessageAndVersion = segmentSnapshotTransform.toApiMessageAndVersion((RemoteLogSegmentMetadataSnapshot) remoteLogMetadata);
        } else {
            throw new IllegalArgumentException("RemoteLogMetadataTransform for given RemoteStorageMetadata class: " + remoteLogMetadata.getClass()
                    + " does not exist.");
        }

        return bytesApiMessageSerde.serialize(apiMessageAndVersion);
    }

    public RemoteLogMetadata deserialize(byte[] data) {
        ApiMessageAndVersion apiMessageAndVersion = bytesApiMessageSerde.deserialize(data);

        short apiKey = apiMessageAndVersion.message().apiKey();
        if (apiKey == REMOTE_LOG_SEGMENT_METADATA_API_KEY) {
            return segmentTransform.fromApiMessageAndVersion(apiMessageAndVersion);
        } else if (apiKey == REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY) {
            return segmentUpdateTransform.fromApiMessageAndVersion(apiMessageAndVersion);
        } else if (apiKey == REMOTE_PARTITION_DELETE_API_KEY) {
            return partitionDeleteTransform.fromApiMessageAndVersion(apiMessageAndVersion);
        } else if (apiKey == REMOTE_LOG_SEGMENT_METADATA_SNAPSHOT_API_KEY) {
            return segmentSnapshotTransform.fromApiMessageAndVersion(apiMessageAndVersion);
        } else {
            throw new IllegalArgumentException("RemoteLogMetadataTransform for apikey: " + apiKey + " does not exist.");
        }

    }

    public static class RemoteLogMetadataFormatter implements MessageFormatter {
        private final RemoteLogMetadataSerde remoteLogMetadataSerde = new RemoteLogMetadataSerde();

        @Override
        public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
            // The key is expected to be null.
            output.printf("partition: %d, offset: %d, value: %s%n",
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    remoteLogMetadataSerde.deserialize(consumerRecord.value()).toString());
        }
    }
}
