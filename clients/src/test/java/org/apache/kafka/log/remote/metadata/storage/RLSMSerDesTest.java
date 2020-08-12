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
package org.apache.kafka.log.remote.metadata.storage;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.log.remote.metadata.storage.RLSMSerDe;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

public class RLSMSerDesTest {

    @Test
    public void testSerDes() throws Exception {
        final String topic = "foo";

        // RLSM with context as non-null.
        RemoteLogSegmentMetadata rlsmWithNoContext = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(new TopicPartition("bar", 0), UUID.randomUUID()),
                1000L,
                2000L,
                System.currentTimeMillis() - 10000,
                1,
                System.currentTimeMillis(),
                1000, RemoteLogSegmentMetadata.State.COPY_FINISHED, Collections.emptyMap()
        );
        doTestSerDes(topic, rlsmWithNoContext);

        // RLSM with context as non-null.
        RemoteLogSegmentMetadata rlsmWithContext = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(new TopicPartition("bar", 0), UUID.randomUUID()),
                2000L,
                4000L,
                System.currentTimeMillis() - 10000,
                1,
                System.currentTimeMillis(),
                1000, RemoteLogSegmentMetadata.State.COPY_FINISHED, Collections.emptyMap()
        );
        doTestSerDes(topic, rlsmWithContext);

        //RLSM marked with deletion
        RemoteLogSegmentMetadata rlsmMarkedDelete = new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(new TopicPartition("bar", 0), UUID.randomUUID()),
                2000L,
                4000L,
                System.currentTimeMillis() - 10000,
                1,
                System.currentTimeMillis(),
                1000, RemoteLogSegmentMetadata.State.COPY_FINISHED, Collections.emptyMap()
        );
        doTestSerDes(topic, rlsmMarkedDelete);

    }

    private void doTestSerDes(final String topic, final RemoteLogSegmentMetadata rlsm) {
        try (RLSMSerDe rlsmSerDe = new RLSMSerDe()) {
            rlsmSerDe.configure(Collections.emptyMap(), false);

            final Serializer<RemoteLogSegmentMetadata> serializer = rlsmSerDe.serializer();
            final byte[] serializedBytes = serializer.serialize(topic, rlsm);

            final Deserializer<RemoteLogSegmentMetadata> deserializer = rlsmSerDe.deserializer();
            final RemoteLogSegmentMetadata deserializedRlsm = deserializer.deserialize(topic, serializedBytes);

            Assert.assertEquals(rlsm, deserializedRlsm);
        }
    }
}
