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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;

import org.junit.jupiter.params.provider.Arguments;

import java.util.List;
import java.util.stream.Stream;

public class OffsetMessageFormatterTest extends CoordinatorRecordMessageFormatterTest {
    private static final OffsetCommitKey OFFSET_COMMIT_KEY = new OffsetCommitKey()
        .setGroup("group-id")
        .setTopic("foo")
        .setPartition(1);
    private static final OffsetCommitValue OFFSET_COMMIT_VALUE = new OffsetCommitValue()
        .setOffset(100L)
        .setLeaderEpoch(10)
        .setMetadata("metadata")
        .setCommitTimestamp(1234L)
        .setExpireTimestamp(5678L)
        .setTopicId(Uuid.fromString("MKXx1fIkQy2J9jXHhK8m1w"));
    private static final GroupMetadataKey GROUP_METADATA_KEY = new GroupMetadataKey().setGroup("group-id");
    private static final GroupMetadataValue GROUP_METADATA_VALUE = new GroupMetadataValue()
        .setProtocolType("consumer")
        .setGeneration(1)
        .setProtocol("range")
        .setLeader("leader")
        .setMembers(List.of());

    @Override
    protected CoordinatorRecordMessageFormatter formatter() {
        return new OffsetsMessageFormatter();
    }

    @Override
    protected Stream<Arguments> parameters() {
        return Stream.of(
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_VALUE).array(),
                """
                    {"key":{"type":0,"data":{"group":"group-id","topic":"foo","partition":1}},
                     "value":{"version":0,
                              "data":{"offset":100,
                                      "metadata":"metadata",
                                      "commitTimestamp":1234}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_VALUE).array(),
                """
                    {"key":{"type":1,"data":{"group":"group-id","topic":"foo","partition":1}},
                     "value":{"version":0,
                              "data":{"offset":100,
                                      "metadata":"metadata",
                                      "commitTimestamp":1234}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_VALUE).array(),
                """
                    {"key":{"type":1,"data":{"group":"group-id","topic":"foo","partition":1}},
                     "value":{"version":1,
                              "data":{"offset":100,
                                      "metadata":"metadata",
                                      "commitTimestamp":1234,
                                      "expireTimestamp":5678}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 2, OFFSET_COMMIT_VALUE).array(),
                """
                    {"key":{"type":1,"data":{"group":"group-id","topic":"foo","partition":1}},
                     "value":{"version":2,
                              "data":{"offset":100,
                                      "metadata":"metadata",
                                      "commitTimestamp":1234}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 3, OFFSET_COMMIT_VALUE).array(),
                """
                    {"key":{"type":1,"data":{"group":"group-id","topic":"foo","partition":1}},
                     "value":{"version":3,
                              "data":{"offset":100,
                                      "leaderEpoch":10,
                                      "metadata":"metadata",
                                      "commitTimestamp":1234}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 4, OFFSET_COMMIT_VALUE).array(),
                """
                    {"key":{"type":1,"data":{"group":"group-id","topic":"foo","partition":1}},
                     "value":{"version":4,
                              "data":{"offset":100,
                                      "leaderEpoch":10,
                                      "metadata":"metadata",
                                      "commitTimestamp":1234,
                                      "topicId":"MKXx1fIkQy2J9jXHhK8m1w"}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_KEY).array(),
                null,
                """
                  {"key":{"type":1,"data":{"group":"group-id","topic":"foo","partition":1}},"value":null}
                """
            ),
            Arguments.of(
                null,
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_VALUE).array(),
                ""
            ),
            Arguments.of(null, null, ""),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_VALUE).array(),
                ""
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer(Short.MAX_VALUE, GROUP_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_VALUE).array(),
                ""
            )
        );
    }
}
