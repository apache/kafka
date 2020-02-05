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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageTestUtil;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.apache.kafka.common.protocol.ApiKeys.STOP_REPLICA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StopReplicaRequestTest {

    @Test
    public void testUnsupportedVersion() {
        StopReplicaRequest.Builder builder = new StopReplicaRequest.Builder(
                (short) (STOP_REPLICA.latestVersion() + 1),
                0, 0, 0L, false, Collections.emptyList());
        assertThrows(UnsupportedVersionException.class, builder::build);
    }

    @Test
    public void testGetErrorResponse() {
        for (short version = STOP_REPLICA.oldestVersion(); version < STOP_REPLICA.latestVersion(); version++) {
            StopReplicaRequest.Builder builder = new StopReplicaRequest.Builder(version,
                    0, 0, 0L, false, Collections.emptyList());
            StopReplicaRequest request = builder.build();
            StopReplicaResponse response = request.getErrorResponse(0,
                    new ClusterAuthorizationException("Not authorized"));
            assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error());
        }
    }

    @Test
    public void testStopReplicaRequestNormalization() {
        Set<TopicPartition> tps = TestUtils.generateRandomTopicPartitions(10, 10);
        StopReplicaRequest.Builder builder = new StopReplicaRequest.Builder((short) 5, 0, 0, 0, false, tps);
        assertTrue(MessageTestUtil.messageSize(builder.build((short) 1).data(), (short) 1) <
            MessageTestUtil.messageSize(builder.build((short) 0).data(), (short) 0));
    }

}
