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

package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.storage.log.FetchIsolation;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RemoteStorageFetchInfoTest {
    private final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");

    @Test
    public void testClientIdDefaultsToEmpty() {
        RemoteStorageFetchInfo info = new RemoteStorageFetchInfo(
            100, false, topicIdPartition, null, FetchIsolation.HIGH_WATERMARK);
        assertEquals(Optional.empty(), info.clientId());
    }

    @Test
    public void testClientIdRetained() {
        RemoteStorageFetchInfo info = new RemoteStorageFetchInfo(
            100, false, topicIdPartition, null, FetchIsolation.HIGH_WATERMARK, Optional.of("client-1"));
        assertEquals(Optional.of("client-1"), info.clientId());
    }
}
