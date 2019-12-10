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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SubscriptionInfoTest {
    private final UUID processId = UUID.randomUUID();
    private final Set<TaskId> activeTasks = new HashSet<>(Arrays.asList(
        new TaskId(0, 0),
        new TaskId(0, 1),
        new TaskId(1, 0)));
    private final Set<TaskId> standbyTasks = new HashSet<>(Arrays.asList(
        new TaskId(1, 1),
        new TaskId(2, 0)));

    private final static String IGNORED_USER_ENDPOINT = "ignoredUserEndpoint:80";

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowForUnknownVersion1() {
        new SubscriptionInfo(
            0,
            LATEST_SUPPORTED_VERSION,
            processId,
            activeTasks,
            standbyTasks,
            "localhost:80"
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowForUnknownVersion2() {
        new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION + 1,
            LATEST_SUPPORTED_VERSION,
            processId,
            activeTasks,
            standbyTasks,
            "localhost:80"
        );
    }

    @Test
    public void shouldEncodeAndDecodeVersion1() {
        final SubscriptionInfo info = new SubscriptionInfo(
            1,
            LATEST_SUPPORTED_VERSION,
            processId,
            activeTasks,
            standbyTasks,
            IGNORED_USER_ENDPOINT
        );
        final SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());
        assertEquals(1, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(processId, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertNull(decoded.userEndPoint());
    }

    @Test
    public void generatedVersion1ShouldBeDecodableByLegacyLogic() {
        final SubscriptionInfo info = new SubscriptionInfo(
            1,
            1234,
            processId,
            activeTasks,
            standbyTasks,
            "ignoreme"
        );
        final ByteBuffer buffer = info.encode();

        final LegacySubscriptionInfoSerde decoded = LegacySubscriptionInfoSerde.decode(buffer);
        assertEquals(1, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(processId, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertNull(decoded.userEndPoint());
    }


    @Test
    public void generatedVersion1ShouldDecodeLegacyFormat() {
        final LegacySubscriptionInfoSerde info = new LegacySubscriptionInfoSerde(
            1,
            LATEST_SUPPORTED_VERSION,
            processId,
            activeTasks,
            standbyTasks,
            "localhost:80"
        );
        final ByteBuffer buffer = info.encode();
        buffer.rewind();
        final SubscriptionInfo decoded = SubscriptionInfo.decode(buffer);
        assertEquals(1, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(processId, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertNull(decoded.userEndPoint());
    }

    @Test
    public void shouldEncodeAndDecodeVersion2() {
        final SubscriptionInfo info = new SubscriptionInfo(
            2,
            LATEST_SUPPORTED_VERSION,
            processId,
            activeTasks,
            standbyTasks,
            "localhost:80"
        );
        final SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());
        assertEquals(2, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(processId, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertEquals("localhost:80", decoded.userEndPoint());
    }

    @Test
    public void generatedVersion2ShouldBeDecodableByLegacyLogic() {
        final SubscriptionInfo info = new SubscriptionInfo(
            2,
            LATEST_SUPPORTED_VERSION,
            processId,
            activeTasks,
            standbyTasks,
            "localhost:80"
        );
        final ByteBuffer buffer = info.encode();

        final LegacySubscriptionInfoSerde decoded = LegacySubscriptionInfoSerde.decode(buffer);
        assertEquals(2, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(processId, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertEquals("localhost:80", decoded.userEndPoint());
    }

    @Test
    public void generatedVersion2ShouldDecodeLegacyFormat() {
        final LegacySubscriptionInfoSerde info = new LegacySubscriptionInfoSerde(
            2,
            LATEST_SUPPORTED_VERSION,
            processId,
            activeTasks,
            standbyTasks,
            "localhost:80"
        );
        final ByteBuffer buffer = info.encode();
        buffer.rewind();
        final SubscriptionInfo decoded = SubscriptionInfo.decode(buffer);
        assertEquals(2, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(processId, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertEquals("localhost:80", decoded.userEndPoint());
    }

    @Test
    public void shouldEncodeAndDecodeVersion3And4() {
        for (int version = 3; version <= 4; version++) {
            final SubscriptionInfo info = new SubscriptionInfo(
                version,
                LATEST_SUPPORTED_VERSION,
                processId,
                activeTasks,
                standbyTasks,
                "localhost:80"
            );
            final SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());
            assertEquals(version, decoded.version());
            assertEquals(LATEST_SUPPORTED_VERSION, decoded.latestSupportedVersion());
            assertEquals(processId, decoded.processId());
            assertEquals(activeTasks, decoded.prevTasks());
            assertEquals(standbyTasks, decoded.standbyTasks());
            assertEquals("localhost:80", decoded.userEndPoint());
        }
    }

    @Test
    public void generatedVersion3And4ShouldBeDecodableByLegacyLogic() {
        for (int version = 3; version <= 4; version++) {
            final SubscriptionInfo info = new SubscriptionInfo(
                version,
                LATEST_SUPPORTED_VERSION,
                processId,
                activeTasks,
                standbyTasks,
                "localhost:80"
            );
            final ByteBuffer buffer = info.encode();

            final LegacySubscriptionInfoSerde decoded = LegacySubscriptionInfoSerde.decode(buffer);
            assertEquals(version, decoded.version());
            assertEquals(LATEST_SUPPORTED_VERSION, decoded.latestSupportedVersion());
            assertEquals(processId, decoded.processId());
            assertEquals(activeTasks, decoded.prevTasks());
            assertEquals(standbyTasks, decoded.standbyTasks());
            assertEquals("localhost:80", decoded.userEndPoint());
        }
    }

    @Test
    public void generatedVersion3To5ShouldDecodeLegacyFormat() {
        for (int version = 3; version <= 5; version++) {
            final LegacySubscriptionInfoSerde info = new LegacySubscriptionInfoSerde(
                version,
                LATEST_SUPPORTED_VERSION,
                processId,
                activeTasks,
                standbyTasks,
                "localhost:80"
            );
            final ByteBuffer buffer = info.encode();
            buffer.rewind();
            final SubscriptionInfo decoded = SubscriptionInfo.decode(buffer);
            final String message = "for version: " + version;
            assertEquals(message, version, decoded.version());
            assertEquals(message, LATEST_SUPPORTED_VERSION, decoded.latestSupportedVersion());
            assertEquals(message, processId, decoded.processId());
            assertEquals(message, activeTasks, decoded.prevTasks());
            assertEquals(message, standbyTasks, decoded.standbyTasks());
            assertEquals(message, "localhost:80", decoded.userEndPoint());
        }
    }

    @Test
    public void shouldEncodeAndDecodeVersion5() {
        final SubscriptionInfo info = new SubscriptionInfo(5, LATEST_SUPPORTED_VERSION, processId, activeTasks, standbyTasks, "localhost:80");
        assertEquals(info, SubscriptionInfo.decode(info.encode()));
    }

    @Test
    public void shouldAllowToDecodeFutureSupportedVersion() {
        final SubscriptionInfo info = SubscriptionInfo.decode(encodeFutureVersion());
        assertEquals(LATEST_SUPPORTED_VERSION + 1, info.version());
        assertEquals(LATEST_SUPPORTED_VERSION + 1, info.latestSupportedVersion());
    }

    @Test
    public void shouldEncodeAndDecodeSmallerLatestSupportedVersion() {
        final int usedVersion = LATEST_SUPPORTED_VERSION - 1;
        final int latestSupportedVersion = LATEST_SUPPORTED_VERSION - 1;

        final SubscriptionInfo info = new SubscriptionInfo(usedVersion, latestSupportedVersion, processId, activeTasks, standbyTasks, "localhost:80");
        final SubscriptionInfo expectedInfo = new SubscriptionInfo(usedVersion, latestSupportedVersion, processId, activeTasks, standbyTasks, "localhost:80");
        assertEquals(expectedInfo, SubscriptionInfo.decode(info.encode()));
    }

    private static ByteBuffer encodeFutureVersion() {
        final ByteBuffer buf = ByteBuffer.allocate(4 /* used version */
                                                       + 4 /* supported version */);
        buf.putInt(LATEST_SUPPORTED_VERSION + 1);
        buf.putInt(LATEST_SUPPORTED_VERSION + 1);
        buf.rewind();
        return buf;
    }

}
