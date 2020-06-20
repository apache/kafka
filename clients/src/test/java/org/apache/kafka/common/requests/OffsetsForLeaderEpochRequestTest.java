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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class OffsetsForLeaderEpochRequestTest {

    @Test
    public void testForConsumerRequiresVersion3() {
        OffsetsForLeaderEpochRequest.Builder builder = OffsetsForLeaderEpochRequest.Builder.forConsumer(Collections.emptyMap());
        for (short version = 0; version < 3; version++) {
            final short v = version;
            assertThrows(UnsupportedVersionException.class, () -> builder.build(v));
        }

        for (short version = 3; version < ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(); version++) {
            OffsetsForLeaderEpochRequest request = builder.build((short) 3);
            assertEquals(OffsetsForLeaderEpochRequest.CONSUMER_REPLICA_ID, request.replicaId());
        }
    }

    @Test
    public void testDefaultReplicaId() {
        for (short version = 0; version < ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(); version++) {
            int replicaId = 1;
            OffsetsForLeaderEpochRequest.Builder builder = OffsetsForLeaderEpochRequest.Builder.forFollower(
                    version, Collections.emptyMap(), replicaId);
            OffsetsForLeaderEpochRequest request = builder.build();
            OffsetsForLeaderEpochRequest parsed = (OffsetsForLeaderEpochRequest) AbstractRequest.parseRequest(
                    ApiKeys.OFFSET_FOR_LEADER_EPOCH, version, request.toStruct());
            if (version < 3)
                assertEquals(OffsetsForLeaderEpochRequest.DEBUGGING_REPLICA_ID, parsed.replicaId());
            else
                assertEquals(replicaId, parsed.replicaId());
        }
    }

}