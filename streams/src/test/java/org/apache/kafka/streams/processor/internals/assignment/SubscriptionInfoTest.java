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

import java.util.Map;

import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CLIENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T1_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T1_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T2_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T2_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.MIN_NAMED_TOPOLOGY_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.MIN_VERSION_OFFSET_SUM_SUBSCRIPTION;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class SubscriptionInfoTest {
    private static final Set<TaskId> ACTIVE_TASKS = new HashSet<>(Arrays.asList(
        TASK_0_0,
        TASK_0_1,
        TASK_1_0));
    private static final Set<TaskId> STANDBY_TASKS = new HashSet<>(Arrays.asList(
        TASK_1_1,
        TASK_2_0));
    private static final Map<TaskId, Long> TASK_OFFSET_SUMS = mkMap(
        mkEntry(TASK_0_0, Task.LATEST_OFFSET),
        mkEntry(TASK_0_1, Task.LATEST_OFFSET),
        mkEntry(TASK_1_0, Task.LATEST_OFFSET),
        mkEntry(TASK_1_1, 0L),
        mkEntry(TASK_2_0, 10L)
    );

    private static final Map<TaskId, Long> NAMED_TASK_OFFSET_SUMS = mkMap(
        mkEntry(NAMED_TASK_T0_0_0, Task.LATEST_OFFSET),
        mkEntry(NAMED_TASK_T0_0_1, Task.LATEST_OFFSET),
        mkEntry(NAMED_TASK_T0_1_0, 5L),
        mkEntry(NAMED_TASK_T0_1_1, 10_000L),
        mkEntry(NAMED_TASK_T1_0_0, Task.LATEST_OFFSET),
        mkEntry(NAMED_TASK_T1_0_1, 0L),
        mkEntry(NAMED_TASK_T2_0_0, 10L),
        mkEntry(NAMED_TASK_T2_2_0, 5L)
        );
    private static final Map<String, String> CLIENT_TAGS = mkMap(mkEntry("t1", "v1"), mkEntry("t2", "v2"));

    private final static String IGNORED_USER_ENDPOINT = "ignoredUserEndpoint:80";
    private static final byte IGNORED_UNIQUE_FIELD = (byte) 0;
    private static final int IGNORED_ERROR_CODE = 0;

    @Test
    public void shouldThrowForUnknownVersion1() {
        assertThrows(IllegalArgumentException.class, () -> new SubscriptionInfo(
            0,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            "localhost:80",
            TASK_OFFSET_SUMS,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE,
            EMPTY_CLIENT_TAGS
        ));
    }

    @Test
    public void shouldThrowForUnknownVersion2() {
        assertThrows(IllegalArgumentException.class, () -> new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION + 1,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            "localhost:80",
            TASK_OFFSET_SUMS,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE,
            EMPTY_CLIENT_TAGS
        ));
    }

    @Test
    public void shouldEncodeAndDecodeVersion1() {
        final SubscriptionInfo info = new SubscriptionInfo(
            1,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            IGNORED_USER_ENDPOINT,
            TASK_OFFSET_SUMS,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE,
            EMPTY_CLIENT_TAGS
        );
        final SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());
        assertEquals(1, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(ACTIVE_TASKS, decoded.prevTasks());
        assertEquals(STANDBY_TASKS, decoded.standbyTasks());
        assertNull(decoded.userEndPoint());
    }

    @Test
    public void generatedVersion1ShouldBeDecodableByLegacyLogic() {
        final SubscriptionInfo info = new SubscriptionInfo(
            1,
            1234,
            UUID_1,
            "ignoreme",
            TASK_OFFSET_SUMS,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE,
            EMPTY_CLIENT_TAGS
        );
        final ByteBuffer buffer = info.encode();

        final LegacySubscriptionInfoSerde decoded = LegacySubscriptionInfoSerde.decode(buffer);
        assertEquals(1, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(ACTIVE_TASKS, decoded.prevTasks());
        assertEquals(STANDBY_TASKS, decoded.standbyTasks());
        assertNull(decoded.userEndPoint());
    }

    @Test
    public void generatedVersion1ShouldDecodeLegacyFormat() {
        final LegacySubscriptionInfoSerde info = new LegacySubscriptionInfoSerde(
            1,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            ACTIVE_TASKS,
            STANDBY_TASKS,
            "localhost:80"
        );
        final ByteBuffer buffer = info.encode();
        buffer.rewind();
        final SubscriptionInfo decoded = SubscriptionInfo.decode(buffer);
        assertEquals(1, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(ACTIVE_TASKS, decoded.prevTasks());
        assertEquals(STANDBY_TASKS, decoded.standbyTasks());
        assertNull(decoded.userEndPoint());
    }

    @Test
    public void shouldEncodeAndDecodeVersion2() {
        final SubscriptionInfo info = new SubscriptionInfo(
            2,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            "localhost:80",
            TASK_OFFSET_SUMS,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE,
            EMPTY_CLIENT_TAGS
        );
        final SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());
        assertEquals(2, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(ACTIVE_TASKS, decoded.prevTasks());
        assertEquals(STANDBY_TASKS, decoded.standbyTasks());
        assertEquals("localhost:80", decoded.userEndPoint());
    }

    @Test
    public void generatedVersion2ShouldBeDecodableByLegacyLogic() {
        final SubscriptionInfo info = new SubscriptionInfo(
            2,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            "localhost:80",
            TASK_OFFSET_SUMS,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE,
            EMPTY_CLIENT_TAGS
        );
        final ByteBuffer buffer = info.encode();

        final LegacySubscriptionInfoSerde decoded = LegacySubscriptionInfoSerde.decode(buffer);
        assertEquals(2, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(ACTIVE_TASKS, decoded.prevTasks());
        assertEquals(STANDBY_TASKS, decoded.standbyTasks());
        assertEquals("localhost:80", decoded.userEndPoint());
    }

    @Test
    public void generatedVersion2ShouldDecodeLegacyFormat() {
        final LegacySubscriptionInfoSerde info = new LegacySubscriptionInfoSerde(
            2,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            ACTIVE_TASKS,
            STANDBY_TASKS,
            "localhost:80"
        );
        final ByteBuffer buffer = info.encode();
        buffer.rewind();
        final SubscriptionInfo decoded = SubscriptionInfo.decode(buffer);
        assertEquals(2, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(ACTIVE_TASKS, decoded.prevTasks());
        assertEquals(STANDBY_TASKS, decoded.standbyTasks());
        assertEquals("localhost:80", decoded.userEndPoint());
    }

    @Test
    public void shouldEncodeAndDecodeVersion3And4() {
        for (int version = 3; version <= 4; version++) {
            final SubscriptionInfo info = new SubscriptionInfo(
                version,
                LATEST_SUPPORTED_VERSION,
                UUID_1,
                "localhost:80",
                TASK_OFFSET_SUMS,
                IGNORED_UNIQUE_FIELD,
                IGNORED_ERROR_CODE,
                EMPTY_CLIENT_TAGS
            );
            final SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());
            assertEquals(version, decoded.version());
            assertEquals(LATEST_SUPPORTED_VERSION, decoded.latestSupportedVersion());
            assertEquals(UUID_1, decoded.processId());
            assertEquals(ACTIVE_TASKS, decoded.prevTasks());
            assertEquals(STANDBY_TASKS, decoded.standbyTasks());
            assertEquals("localhost:80", decoded.userEndPoint());
        }
    }

    @Test
    public void generatedVersion3And4ShouldBeDecodableByLegacyLogic() {
        for (int version = 3; version <= 4; version++) {
            final SubscriptionInfo info = new SubscriptionInfo(
                version,
                LATEST_SUPPORTED_VERSION,
                UUID_1,
                "localhost:80",
                TASK_OFFSET_SUMS,
                IGNORED_UNIQUE_FIELD,
                IGNORED_ERROR_CODE,
                EMPTY_CLIENT_TAGS
            );
            final ByteBuffer buffer = info.encode();

            final LegacySubscriptionInfoSerde decoded = LegacySubscriptionInfoSerde.decode(buffer);
            assertEquals(version, decoded.version());
            assertEquals(LATEST_SUPPORTED_VERSION, decoded.latestSupportedVersion());
            assertEquals(UUID_1, decoded.processId());
            assertEquals(ACTIVE_TASKS, decoded.prevTasks());
            assertEquals(STANDBY_TASKS, decoded.standbyTasks());
            assertEquals("localhost:80", decoded.userEndPoint());
        }
    }

    @Test
    public void generatedVersion3To6ShouldDecodeLegacyFormat() {
        for (int version = 3; version <= 6; version++) {
            final LegacySubscriptionInfoSerde info = new LegacySubscriptionInfoSerde(
                version,
                LATEST_SUPPORTED_VERSION,
                UUID_1,
                ACTIVE_TASKS,
                STANDBY_TASKS,
                "localhost:80"
            );
            final ByteBuffer buffer = info.encode();
            buffer.rewind();
            final SubscriptionInfo decoded = SubscriptionInfo.decode(buffer);
            final String message = "for version: " + version;
            assertEquals(message, version, decoded.version());
            assertEquals(message, LATEST_SUPPORTED_VERSION, decoded.latestSupportedVersion());
            assertEquals(message, UUID_1, decoded.processId());
            assertEquals(message, ACTIVE_TASKS, decoded.prevTasks());
            assertEquals(message, STANDBY_TASKS, decoded.standbyTasks());
            assertEquals(message, "localhost:80", decoded.userEndPoint());
        }
    }

    @Test
    public void shouldEncodeAndDecodeVersion5() {
        final SubscriptionInfo info =
            new SubscriptionInfo(5, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
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

        final SubscriptionInfo info =
            new SubscriptionInfo(usedVersion, latestSupportedVersion, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        final SubscriptionInfo expectedInfo =
            new SubscriptionInfo(usedVersion, latestSupportedVersion, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        assertEquals(expectedInfo, SubscriptionInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion7() {
        final SubscriptionInfo info =
            new SubscriptionInfo(7, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldConvertTaskOffsetSumMapToTaskSets() {
        final SubscriptionInfo info =
            new SubscriptionInfo(7, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        assertThat(info.prevTasks(), is(ACTIVE_TASKS));
        assertThat(info.standbyTasks(), is(STANDBY_TASKS));
    }

    @Test
    public void shouldReturnTaskOffsetSumsMapForDecodedSubscription() {
        final SubscriptionInfo info = SubscriptionInfo.decode(
            new SubscriptionInfo(MIN_VERSION_OFFSET_SUM_SUBSCRIPTION,
                                 LATEST_SUPPORTED_VERSION, UUID_1,
                                 "localhost:80",
                                 TASK_OFFSET_SUMS,
                                 IGNORED_UNIQUE_FIELD,
                                 IGNORED_ERROR_CODE,
                                 EMPTY_CLIENT_TAGS).encode());
        assertThat(info.taskOffsetSums(), is(TASK_OFFSET_SUMS));
    }

    @Test
    public void shouldConvertTaskSetsToTaskOffsetSumMapWithOlderSubscription() {
        final Map<TaskId, Long> expectedOffsetSumsMap = mkMap(
            mkEntry(new TaskId(0, 0), Task.LATEST_OFFSET),
            mkEntry(new TaskId(0, 1), Task.LATEST_OFFSET),
            mkEntry(new TaskId(1, 0), Task.LATEST_OFFSET),
            mkEntry(new TaskId(1, 1), UNKNOWN_OFFSET_SUM),
            mkEntry(new TaskId(2, 0), UNKNOWN_OFFSET_SUM)
        );

        final SubscriptionInfo info = SubscriptionInfo.decode(
            new LegacySubscriptionInfoSerde(
                SubscriptionInfo.MIN_VERSION_OFFSET_SUM_SUBSCRIPTION - 1,
                LATEST_SUPPORTED_VERSION,
                UUID_1,
                ACTIVE_TASKS,
                STANDBY_TASKS,
                "localhost:80")
            .encode());

        assertThat(info.taskOffsetSums(), is(expectedOffsetSumsMap));
    }

    @Test
    public void shouldEncodeAndDecodeVersion8() {
        final SubscriptionInfo info =
            new SubscriptionInfo(8, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldNotErrorAccessingFutureVars() {
        final SubscriptionInfo info =
                new SubscriptionInfo(8, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        try {
            info.errorCode();
        } catch (final Exception e) {
            fail("should not error");
        }
    }

    @Test
    public void shouldEncodeAndDecodeVersion9() {
        final SubscriptionInfo info =
                new SubscriptionInfo(9, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldEncodeAndDecodeVersion10() {
        final SubscriptionInfo info =
            new SubscriptionInfo(10, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldEncodeAndDecodeVersion10WithNamedTopologies() {
        final SubscriptionInfo info =
            new SubscriptionInfo(10, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", NAMED_TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldThrowIfAttemptingToUseNamedTopologiesWithOlderVersion() {
        assertThrows(
            TaskAssignmentException.class,
            () -> new SubscriptionInfo(MIN_NAMED_TOPOLOGY_VERSION - 1, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", NAMED_TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS)
        );
    }

    @Test
    public void shouldEncodeAndDecodeVersion11() {
        final SubscriptionInfo info =
            new SubscriptionInfo(11, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, CLIENT_TAGS);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldReturnEncodeDecodeEmptyClientTagsOnVersion11() {
        final SubscriptionInfo info =
            new SubscriptionInfo(11, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, EMPTY_CLIENT_TAGS);
        assertThat(info.clientTags(), is(anEmptyMap()));
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldReturnEmptyMapOfClientTagsOnOlderVersions() {
        final SubscriptionInfo info =
            new SubscriptionInfo(10, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, CLIENT_TAGS);

        assertThat(info.clientTags(), is(anEmptyMap()));
    }

    @Test
    public void shouldReturnMapOfClientTagsOnVersion11() {
        final SubscriptionInfo info =
            new SubscriptionInfo(11, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", TASK_OFFSET_SUMS, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE, CLIENT_TAGS);

        assertThat(info.clientTags(), is(CLIENT_TAGS));
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
