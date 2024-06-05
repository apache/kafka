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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState.COPY_SEGMENT_STARTED;

public class RemoteLogMetadataFormatterTest {
    private final Uuid topicId = Uuid.randomUuid();
    private final String topic = "foo";
    private final TopicIdPartition tpId0 = new TopicIdPartition(topicId, new TopicPartition(topic, 0));
    private final Uuid segmentId = Uuid.randomUuid();

    @Test
    public void testFormat() throws IOException {
        Map<Integer, Long> segLeaderEpochs = new HashMap<>();
        segLeaderEpochs.put(0, 0L);
        segLeaderEpochs.put(1, 20L);
        segLeaderEpochs.put(2, 80L);
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(tpId0, segmentId);
        Optional<CustomMetadata> customMetadata = Optional.of(new CustomMetadata(new byte[10]));
        RemoteLogSegmentMetadata remoteLogMetadata = new RemoteLogSegmentMetadata(
                remoteLogSegmentId, 0L, 100L, -1L, 1, 123L, 1024, customMetadata, COPY_SEGMENT_STARTED,
                segLeaderEpochs);

        byte[] metadataBytes = new RemoteLogMetadataSerde().serialize(remoteLogMetadata);
        ConsumerRecord<byte[], byte[]> metadataRecord = new ConsumerRecord<>(
                "__remote_log_metadata", 0, 0, null, metadataBytes);

        String expected = String.format(
                "partition: 0, offset: 0, value: " +
                        "RemoteLogSegmentMetadata{remoteLogSegmentId=RemoteLogSegmentId{topicIdPartition=%s:foo-0, id=%s}, " +
                        "startOffset=0, endOffset=100, brokerId=1, maxTimestampMs=-1, " +
                        "eventTimestampMs=123, segmentLeaderEpochs={0=0, 1=20, 2=80}, segmentSizeInBytes=1024, " +
                        "customMetadata=Optional[CustomMetadata{10 bytes}], " +
                        "state=COPY_SEGMENT_STARTED}\n",
                topicId, segmentId);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             PrintStream ps = new PrintStream(baos)) {
            try (RemoteLogMetadataSerde.RemoteLogMetadataFormatter formatter =
                         new RemoteLogMetadataSerde.RemoteLogMetadataFormatter()) {
                formatter.writeTo(metadataRecord, ps);
                assertEquals(expected, baos.toString());
            }
        }
    }
}
