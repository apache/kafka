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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.apache.kafka.coordinator.group.streams.TasksTuple;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedRecordsEquals;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TargetAssignmentRecordsBuilderTest {

    private static final Logger LOG = new LogContext().logger(TargetAssignmentRecordsBuilder.class);

    @Test
    public void testEmpty() {
        List<CoordinatorRecord> records =
            new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                .withCurrentMemberIds(Set.of())
                .withCurrentTargetAssignment(Map.of())
                .withNewTargetAssignment(Map.of())
                .build();

        assertEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
        ), records);
    }

    @Test
    public void testAssignmentHasNotChanged() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        List<CoordinatorRecord> records =
            new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                .withCurrentMemberIds(Set.of("member-1", "member-2"))
                .withCurrentTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1, 2)
                    )),
                    "member-2", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 3, 4, 5)
                    ))
                ))
                .withNewTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1, 2)
                    )),
                    "member-2", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 3, 4, 5)
                    ))
                ))
                .build();

        assertEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
        ), records);
    }

    @Test
    public void testAssignmentSwapped() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        List<CoordinatorRecord> records =
            new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                .withCurrentMemberIds(Set.of("member-1", "member-2"))
                .withCurrentTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1, 2)
                    )),
                    "member-2", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 3, 4, 5)
                    ))
                ))
                .withNewTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 3, 4, 5)
                    )),
                    "member-2", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1, 2)
                    ))
                ))
                .build();

        assertUnorderedRecordsEquals(List.of(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 3, 4, 5)
                )),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1, 2)
                ))
            ),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L))
        ), records);
    }

    @Test
    public void testPartialAssignmentUpdate() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        List<CoordinatorRecord> records =
            new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                .withCurrentMemberIds(Set.of("member-1", "member-2", "member-3"))
                .withCurrentTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(barTopicId, 0, 1)
                    )),
                    "member-2", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2, 3),
                        mkTopicAssignment(barTopicId, 2, 3)
                    )),
                    "member-3", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 4, 5),
                        mkTopicAssignment(barTopicId, 4, 5)
                    ))
                ))
                .withNewTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(barTopicId, 0, 1)
                    )),
                    "member-2", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2, 3, 4),
                        mkTopicAssignment(barTopicId, 2, 3, 4)
                    )),
                    "member-3", new Assignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 5),
                        mkTopicAssignment(barTopicId, 5)
                    ))
                ))
                .build();

        assertUnorderedRecordsEquals(List.of(
            List.of(
                // Member 1 has no record because its assignment did not change.
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment(
                    mkTopicAssignment(fooTopicId, 2, 3, 4),
                    mkTopicAssignment(barTopicId, 2, 3, 4)
                )),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                    mkTopicAssignment(fooTopicId, 5),
                    mkTopicAssignment(barTopicId, 5)
                ))
            ),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L))
        ), records);
    }

    @Test
    public void testAssignmentEmptyWhenOmitted() {
        List<CoordinatorRecord> records =
            new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                .withCurrentMemberIds(Set.of("member-1"))
                .withCurrentTargetAssignment(Map.of())
                // The assignor did not include an entry for member 1.
                .withNewTargetAssignment(Map.of())
                .build();

        assertEquals(List.of(
            // An empty target assignment is written for member even though the assignor did not include an entry for it.
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
        ), records);
    }

    @Test
    public void testMemberLeft() {
        Uuid topicId = Uuid.randomUuid();

        List<CoordinatorRecord> records =
            new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                // Member 2 has left.
                .withCurrentMemberIds(Set.of("member-1"))
                .withCurrentTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId, 0, 1, 2, 3)
                    ))
                ))
                .withNewTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId, 0, 1)
                    )),
                    "member-2", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId, 2, 3)
                    ))
                ))
                .build();

        assertEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                mkTopicAssignment(topicId, 0, 1)
            )),
            // No record is written for member 2.
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
        ), records);
    }

    @Test
    public void testMemberJoined() {
        // This test is equivalent to testAssignmentEmptyWhenOmitted.

        Uuid topicId = Uuid.randomUuid();

        List<CoordinatorRecord> records =
            new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                // Member 2 has joined.
                .withCurrentMemberIds(Set.of("member-1", "member-2"))
                .withCurrentTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId, 0, 1, 2)
                    ))
                ))
                .withNewTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId, 0, 1, 2)
                    ))
                ))
                .build();

        assertEquals(List.of(
            // An empty target assignment is written for member 2.
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
        ), records);
    }

    @Test
    public void testStaticMemberReplaced() {
        Uuid topicId = Uuid.randomUuid();

        List<CoordinatorRecord> records =
            new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                .withCurrentMemberIds(Set.of("member-1", "member-3"))
                // Static member 2 has been replaced with member 3.
                .withPreviousStaticMembers(Map.of("instance-id", "member-2"))
                .withCurrentStaticMembers(Map.of("instance-id", "member-3"))
                .withCurrentTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId, 0, 1, 2)
                    )),
                    "member-3", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId, 3, 4, 5)
                    ))
                ))
                .withNewTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId, 0, 1, 2)
                    )),
                    "member-2", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId, 3, 4, 5, 6)
                    ))
                ))
                .build();

        assertEquals(List.of(
            // Member 2's assignment is given to member 3.
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-3", mkAssignment(
                mkTopicAssignment(topicId, 3, 4, 5, 6)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
        ), records);
    }

    @Test
    public void testStaticMemberAndMemberIdConflict() {
        // This scenario will never happen when using the official Java client and may be forbidden
        // in the future.
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        List<CoordinatorRecord> records =
            new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                // instance-id-1 has "moved" from member 1 to member 2.
                //   member 1 has been around the whole time and member 2 is new.
                // instance-id-2 has "moved" from member 3 to member 4.
                //   member 3 left and member 4 has been around the whole time.
                .withCurrentMemberIds(Set.of("member-1", "member-2", "member-4"))
                .withPreviousStaticMembers(Map.of("instance-id-1", "member-1", "instance-id-2", "member-3"))
                .withCurrentStaticMembers(Map.of("instance-id-1", "member-2", "instance-id-2", "member-4"))
                .withCurrentTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId1, 0, 1, 2)
                    )),
                    // member-3's assignment was removed when they left.
                    "member-4", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId2, 3, 4, 5)
                    ))
                ))
                .withNewTargetAssignment(Map.of(
                    "member-1", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId1, 0, 1, 2, 3)
                    )),
                    "member-3", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId2, 0, 1, 2, 3)
                    )),
                    "member-4", new Assignment(mkAssignment(
                        mkTopicAssignment(topicId2, 4, 5, 6, 7)
                    ))
                ))
                .build();

        assertUnorderedRecordsEquals(List.of(
            List.of(
                // Member 1 gets the target assignment.
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                    mkTopicAssignment(topicId1, 0, 1, 2, 3)
                )),
                // Member 2 does not get member 1's target assignment.
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-2", mkAssignment()),
                // Member 3 gets nothing because it is no longer in the group.
                // Member 4 gets the target assignment.
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-4", mkAssignment(
                    mkTopicAssignment(topicId2, 4, 5, 6, 7)
                ))
            ),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L))
        ), records);
    }

    @Nested
    public class ConsumerTargetAssignmentRecordsBuilderTest {

        @Test
        public void testEmptyMemberAssignment() {
            List<CoordinatorRecord> records =
                new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                    .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                    .withCurrentMemberIds(Set.of("member-1"))
                    .withCurrentTargetAssignment(Map.of())
                    .withNewTargetAssignment(Map.of())
                    .build();

            assertEquals(List.of(
                // An empty target assignment is written for member even though the assignor did not include an entry for it.
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment()),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
            ), records);
        }

        @Test
        public void testRecords() {
            Uuid topicId = Uuid.randomUuid();

            List<CoordinatorRecord> records =
                new TargetAssignmentRecordsBuilder.ConsumerTargetAssignmentRecordsBuilder(LOG, "my-group")
                    .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                    .withCurrentMemberIds(Set.of("member-1"))
                    .withCurrentTargetAssignment(Map.of(
                        "member-1", new Assignment(mkAssignment(mkTopicAssignment(topicId, 0)))
                    ))
                    .withNewTargetAssignment(Map.of(
                        "member-1", new Assignment(mkAssignment(mkTopicAssignment(topicId, 0, 1, 2, 3)))
                    ))
                    .build();

            assertEquals(List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                    mkTopicAssignment(topicId, 0, 1, 2, 3)
                )),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
            ), records);
        }
    }

    @Nested
    public class ShareTargetAssignmentRecordsBuilderTest {

        @Test
        public void testEmptyMemberAssignment() {
            List<CoordinatorRecord> records =
                new TargetAssignmentRecordsBuilder.ShareTargetAssignmentRecordsBuilder(LOG, "my-group")
                    .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                    .withCurrentMemberIds(Set.of("member-1"))
                    .withCurrentTargetAssignment(Map.of())
                    .withNewTargetAssignment(Map.of())
                    .build();

            assertEquals(List.of(
                // An empty target assignment is written for member even though the assignor did not include an entry for it.
                GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment()),
                GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
            ), records);
        }

        @Test
        public void testRecords() {
            Uuid topicId = Uuid.randomUuid();

            List<CoordinatorRecord> records =
                new TargetAssignmentRecordsBuilder.ShareTargetAssignmentRecordsBuilder(LOG, "my-group")
                    .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                    .withCurrentMemberIds(Set.of("member-1"))
                    .withCurrentTargetAssignment(Map.of(
                        "member-1", new Assignment(mkAssignment(mkTopicAssignment(topicId, 0, 1)))
                    ))
                    .withNewTargetAssignment(Map.of(
                        "member-1", new Assignment(mkAssignment(mkTopicAssignment(topicId, 0, 1, 2, 3)))
                    ))
                    .build();

            assertEquals(List.of(
                GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord("my-group", "member-1", mkAssignment(
                    mkTopicAssignment(topicId, 0, 1, 2, 3)
                )),
                GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
            ), records);
        }
    }

    @Nested
    public class StreamsTargetAssignmentRecordsBuilderTest {

        @Test
        public void testEmptyMemberAssignment() {
            List<CoordinatorRecord> records =
                new TargetAssignmentRecordsBuilder.StreamsTargetAssignmentRecordsBuilder(LOG, "my-group")
                    .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                    .withCurrentMemberIds(Set.of("member-1"))
                    .withCurrentTargetAssignment(Map.of())
                    .withNewTargetAssignment(Map.of())
                    .build();

            assertEquals(List.of(
                // An empty target assignment is written for member even though the assignor did not include an entry for it.
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord("my-group", "member-1", TasksTuple.EMPTY),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
            ), records);
        }

        @Test
        public void testRecords() {
            String subtopology1 = "subtopology-1";

            List<CoordinatorRecord> records =
                new TargetAssignmentRecordsBuilder.StreamsTargetAssignmentRecordsBuilder(LOG, "my-group")
                    .withTargetAssignmentMetadata(new TargetAssignmentMetadata(20, 12345L))
                    .withCurrentMemberIds(Set.of("member-1"))
                    .withCurrentTargetAssignment(Map.of(
                        "member-1", mkTasksTuple(TaskRole.ACTIVE,
                            mkTasks(subtopology1, 0)
                        )
                    ))
                    .withNewTargetAssignment(Map.of(
                        "member-1", mkTasksTuple(TaskRole.ACTIVE,
                            mkTasks(subtopology1, 0, 1, 2, 3)
                        )
                    ))
                    .build();

            assertEquals(List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord("my-group", "member-1", mkTasksTuple(TaskRole.ACTIVE,
                    mkTasks(subtopology1, 0, 1, 2, 3)
                )),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord("my-group", 20, 12345L)
            ), records);
        }
    }
}
