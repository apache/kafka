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
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataUpdateRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import java.nio.ByteBuffer;

public class RemoteLogSegmentMetadataUpdateSerdes implements RemoteLogMetadataSerdes<RemoteLogSegmentMetadataUpdate> {

    public Message serialize(RemoteLogSegmentMetadataUpdate data) {
        return new RemoteLogSegmentMetadataUpdateRecord()
                .setRemoteLogSegmentId(createRemoteLogSegmentIdEntry(data))
                .setEventTimestampMs(data.eventTimestampMs())
                .setRemoteLogSegmentState(data.state().id());
    }

    private RemoteLogSegmentMetadataUpdateRecord.RemoteLogSegmentIdEntry createRemoteLogSegmentIdEntry(RemoteLogSegmentMetadataUpdate data) {
        return new RemoteLogSegmentMetadataUpdateRecord.RemoteLogSegmentIdEntry()
                .setId(data.remoteLogSegmentId().id())
                .setTopicIdPartition(
                        new RemoteLogSegmentMetadataUpdateRecord.TopicIdPartitionEntry()
                                .setName(data.remoteLogSegmentId().topicIdPartition().topicPartition().topic())
                                .setPartition(data.remoteLogSegmentId().topicIdPartition().topicPartition().partition())
                                .setId(data.remoteLogSegmentId().topicIdPartition().topicId()));
    }

    public RemoteLogSegmentMetadataUpdate deserialize(byte version, ByteBuffer byteBuffer) {
        RemoteLogSegmentMetadataUpdateRecord record = new RemoteLogSegmentMetadataUpdateRecord(
                new ByteBufferAccessor(byteBuffer), version);
        RemoteLogSegmentMetadataUpdateRecord.RemoteLogSegmentIdEntry entry = record.remoteLogSegmentId();
        TopicIdPartition topicIdPartition = new TopicIdPartition(entry.topicIdPartition().id(),
                new TopicPartition(entry.topicIdPartition().name(), entry.topicIdPartition().partition()));

        return new RemoteLogSegmentMetadataUpdate(new RemoteLogSegmentId(topicIdPartition, entry.id()),
                record.eventTimestampMs(), RemoteLogSegmentState.forId(record.remoteLogSegmentState()), record.brokerId());
    }
}
