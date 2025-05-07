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
package org.apache.kafka.raft;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.DataOutputStreamWritable;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.Snapshots;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

public class RaftTestUtils {
    static ApiMessage roundTripApiMessage(ApiMessage message, short version) {
        final var cache =  new ObjectSerializationCache();
        final var buffer = new ByteArrayOutputStream(message.size(cache, version));

        // Encode the message to a byte array with the given version
        message.write(new DataOutputStreamWritable(new DataOutputStream(buffer)), cache, version);

        // Decode the message from the byte array
        var reader = new ByteBufferAccessor(ByteBuffer.wrap(buffer.toByteArray()));
        message.read(reader, version);

        return message;
    }

    static <T> void writeBootstrapSnapshot(
        ReplicatedLog log,
        Optional<VoterSet> voters,
        KRaftVersion version,
        RecordSerde<T> serde
    ) {
        final var builder = new RecordsSnapshotWriter.Builder()
            .setRawSnapshotWriter(
                log.createNewSnapshotUnchecked(Snapshots.BOOTSTRAP_SNAPSHOT_ID).get()
            )
            .setKraftVersion(version)
            .setVoterSet(voters);

        try (RecordsSnapshotWriter<T> writer = builder.build(serde)) {
            writer.freeze();
        }
    }

    static int majoritySize(int numberOfVoters) {
        return (numberOfVoters / 2) + 1;
    }
}
