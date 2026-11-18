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
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.serialization.AbstractApiMessageSerde;
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
import java.nio.ByteBuffer;

/**
 * This class provides serialization and deserialization for {@link RemoteLogMetadata}. This is the root serde
 * for the messages that are stored in internal remote log metadata topic.
 */
public class RemoteLogMetadataSerde {
    private static final AbstractApiMessageSerde API_MESSAGE_SERDE = new AbstractApiMessageSerde() {
        @Override
        public ApiMessage apiMessageFor(short apiKey) {
            return MetadataRecordType.fromId(apiKey).newMetadataRecord();
        }
    };

    private final RemoteLogSegmentMetadataTransform segmentTransform = new RemoteLogSegmentMetadataTransform();
    private final RemoteLogSegmentMetadataUpdateTransform segmentUpdateTransform = new RemoteLogSegmentMetadataUpdateTransform();
    private final RemotePartitionDeleteMetadataTransform partitionDeleteTransform = new RemotePartitionDeleteMetadataTransform();
    private final RemoteLogSegmentMetadataSnapshotTransform segmentSnapshotTransform = new RemoteLogSegmentMetadataSnapshotTransform();

    public byte[] serialize(RemoteLogMetadata remoteLogMetadata) {
        ApiMessageAndVersion apiMessageAndVersion = toApiMessageAndVersion(remoteLogMetadata);

        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = API_MESSAGE_SERDE.recordSize(apiMessageAndVersion, cache);
        ByteBufferAccessor writable = new ByteBufferAccessor(ByteBuffer.allocate(size));
        API_MESSAGE_SERDE.write(apiMessageAndVersion, cache, writable);
        return writable.buffer().array();
    }

    public RemoteLogMetadata deserialize(byte[] data) {
        ApiMessageAndVersion apiMessageAndVersion = API_MESSAGE_SERDE.read(new ByteBufferAccessor(ByteBuffer.wrap(data)), data.length);

        ApiMessage message = apiMessageAndVersion.message();
        if (message instanceof RemoteLogSegmentMetadataRecord) {
            return segmentTransform.fromApiMessageAndVersion(apiMessageAndVersion);
        } else if (message instanceof RemoteLogSegmentMetadataUpdateRecord) {
            return segmentUpdateTransform.fromApiMessageAndVersion(apiMessageAndVersion);
        } else if (message instanceof RemotePartitionDeleteMetadataRecord) {
            return partitionDeleteTransform.fromApiMessageAndVersion(apiMessageAndVersion);
        } else if (message instanceof RemoteLogSegmentMetadataSnapshotRecord) {
            return segmentSnapshotTransform.fromApiMessageAndVersion(apiMessageAndVersion);
        } else {
            throw new IllegalArgumentException("RemoteLogMetadataTransform for apikey: " + message.apiKey() + " does not exist.");
        }
    }

    private ApiMessageAndVersion toApiMessageAndVersion(RemoteLogMetadata remoteLogMetadata) {
        if (remoteLogMetadata instanceof RemoteLogSegmentMetadata metadata) {
            return segmentTransform.toApiMessageAndVersion(metadata);
        } else if (remoteLogMetadata instanceof RemoteLogSegmentMetadataUpdate metadataUpdate) {
            return segmentUpdateTransform.toApiMessageAndVersion(metadataUpdate);
        } else if (remoteLogMetadata instanceof RemotePartitionDeleteMetadata deleteMetadata) {
            return partitionDeleteTransform.toApiMessageAndVersion(deleteMetadata);
        } else if (remoteLogMetadata instanceof RemoteLogSegmentMetadataSnapshot snapshot) {
            return segmentSnapshotTransform.toApiMessageAndVersion(snapshot);
        } else {
            throw new IllegalArgumentException("RemoteLogMetadataTransform for given RemoteLogMetadata class: " + remoteLogMetadata.getClass()
                    + " does not exist.");
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
