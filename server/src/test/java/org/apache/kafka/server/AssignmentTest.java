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
package org.apache.kafka.server;


import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.TopicIdPartition;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AssignmentTest {
    private static final Uuid TOPIC_ID = Uuid.fromString("rTudty6ITOCcO_ldVyzZYg");
    private static final Uuid DIRECTORY_ID = Uuid.fromString("rzRT8XZaSbKsP6j238zogg");
    private static final MetadataImage TEST_IMAGE;

    static {
        MetadataDelta delta = new MetadataDelta.Builder().
            setImage(MetadataImage.EMPTY).
            build();
        delta.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.IBP_3_8_IV0.featureLevel()));
        delta.replay(new TopicRecord().
            setName("foo").
            setTopicId(TOPIC_ID));
        delta.replay(new PartitionRecord().
            setPartitionId(0).
            setTopicId(TOPIC_ID).
            setReplicas(List.of(0, 1, 2)).
            setIsr(List.of(0, 1, 2)).
            setLeader(1));
        delta.replay(new PartitionRecord().
            setPartitionId(1).
            setTopicId(TOPIC_ID).
            setReplicas(List.of(1, 2, 3)).
            setIsr(List.of(1, 2, 3)).
            setLeader(1));
        TEST_IMAGE = delta.apply(MetadataProvenance.EMPTY);
    }

    static class NoOpRunnable implements Runnable {
        static final NoOpRunnable INSTANCE = new NoOpRunnable();

        @Override
        public void run() {
        }

        @Override
        public String toString() {
            return "NoOpRunnable";
        }
    }

    @Test
    public void testValidAssignment() {
        assertTrue(new Assignment(
            new TopicIdPartition(TOPIC_ID, 0),
            DIRECTORY_ID,
            0,
            NoOpRunnable.INSTANCE).valid(0, TEST_IMAGE));
    }

    @Test
    public void testAssignmentForNonExistentTopicIsNotValid() {
        assertFalse(new Assignment(
            new TopicIdPartition(Uuid.fromString("uuOi4qGPSsuM0QwnYINvOw"), 0),
            DIRECTORY_ID,
            0,
            NoOpRunnable.INSTANCE).valid(0, TEST_IMAGE));
    }

    @Test
    public void testAssignmentForNonExistentPartitionIsNotValid() {
        assertFalse(new Assignment(
            new TopicIdPartition(TOPIC_ID, 2),
            DIRECTORY_ID,
            0,
            NoOpRunnable.INSTANCE).valid(0, TEST_IMAGE));
    }

    @Test
    public void testAssignmentReplicaNotOnBrokerIsNotValid() {
        assertFalse(new Assignment(
            new TopicIdPartition(TOPIC_ID, 0),
            DIRECTORY_ID,
            0,
            NoOpRunnable.INSTANCE).valid(3, TEST_IMAGE));
    }

    @Test
    public void testAssignmentToString() {
        assertEquals("Assignment[topicIdPartition=rTudty6ITOCcO_ldVyzZYg:1, " +
            "directoryId=rzRT8XZaSbKsP6j238zogg, " +
            "submissionTimeNs=123, " +
            "successCallback=NoOpRunnable]",
            new Assignment(new TopicIdPartition(TOPIC_ID, 1),
                DIRECTORY_ID,
                123,
                NoOpRunnable.INSTANCE).toString());
    }
}
