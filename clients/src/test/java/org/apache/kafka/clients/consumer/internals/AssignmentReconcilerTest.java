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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventProcessor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_TOPIC_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentReconcilerTest {

    private ConsumerTestBuilder testBuilder;
    private SubscriptionState subscriptions;
    private ApplicationEventProcessor applicationEventProcessor;
    private BackgroundEventProcessor backgroundEventProcessor;
    private AssignmentReconciler reconciler;

    @AfterEach
    public void tearDown() {
        if (testBuilder != null) {
            testBuilder.close();
        }
    }

    private void setup() {
        setup(new NoOpConsumerRebalanceListener());
    }

    private void setup(ConsumerRebalanceListener listener) {
        testBuilder = new ConsumerTestBuilder(ConsumerTestBuilder.createDefaultGroupInformation());

        // Create our subscriptions and subscribe to the topics.
        subscriptions = testBuilder.subscriptions;
        subscriptions.subscribe(Collections.singleton(DEFAULT_TOPIC_NAME), listener);

        // We need the background event processor to process events from the background thread (to execute
        // the ConsumerRebalanceListener callbacks) and the application event processor to forward the result
        // of the callback execution back to the membership manager.
        applicationEventProcessor = testBuilder.applicationEventProcessor;
        backgroundEventProcessor = testBuilder.backgroundEventProcessor;
        reconciler = testBuilder.assignmentReconciler.orElseThrow(() -> new IllegalStateException("Should be in a group"));
    }

    @Test
    public void testAssignment() {
        setup();

        // Create our initial assignment
        Set<TopicPartition> assignment = newTopicPartitions(0, 1, 2, 3);

        // Start the reconciliation process. At this point, since there are no partitions assigned to our
        // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
        // prematurely assigned anything to the subscriptions.
        reconciler.startReconcile(assignment);
        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());

        // Complete the future to signal to the reconciler that the ConsumerRebalanceListener callback
        // has completed. This will trigger the "commit" of the partition assignment to the subscriptions.
        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());
        backgroundEventProcessor.process();
        applicationEventProcessor.process();
        assertEquals(assignment, subscriptions.assignedPartitions());
    }

    @Test
    public void testAssignmentAndRevocation() {
        setup();

        Set<TopicPartition> originalAssignment = newTopicPartitions(0, 1, 2, 3);

        // Create our initial assignment that adds four partitions
        {
            // Start the reconciliation process. At this point, since there are no partitions assigned to our
            // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
            // prematurely assigned anything to the subscriptions.
            reconciler.startReconcile(originalAssignment);
            assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());

            // Now process the callback.
            backgroundEventProcessor.process();
            applicationEventProcessor.process();
            assertEquals(originalAssignment, subscriptions.assignedPartitions());
        }

        // Create our follow-up assignment that removes two partitions.
        {
            Set<TopicPartition> newAssignment = newTopicPartitions(0, 2);

            // We get another assignment. Since we have partitions assigned, we will need to revoke some
            // old partitions that are no longer part of the new target assignment.
            reconciler.startReconcile(newAssignment);
            assertEquals(originalAssignment, subscriptions.assignedPartitions());

            // Now process the callback.
            backgroundEventProcessor.process();
            applicationEventProcessor.process();
            assertEquals(newAssignment, subscriptions.assignedPartitions());
        }
    }

    @Test
    public void testLose() {
        setup();

        // This mimics having set up an assignment already.
        SortedSet<TopicPartition> partitions = newTopicPartitions(0, 1, 2, 3);
        subscriptions.assignFromSubscribed(partitions);

        assertEquals(partitions, subscriptions.assignedPartitions());
        reconciler.startLost();
        assertEquals(partitions, subscriptions.assignedPartitions());

        backgroundEventProcessor.process();
        applicationEventProcessor.process();
        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());
    }

    @Test
    public void testRevocationFailure() {
        ConsumerRebalanceListener failingListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                throw new KafkaException("Simulating callback failure");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        };

        setup(failingListener);

        // This mimics having set up an assignment of four partitions.
        SortedSet<TopicPartition> partitions = newTopicPartitions(0, 1, 2, 3);
        subscriptions.assignFromSubscribed(partitions);

        // When revoking partitions, we get an error. This should not stop the reconciliation process, though.
        {
            Set<TopicPartition> assignment = newTopicPartitions(0, 2);

            // Start the reconciliation process.
            reconciler.startReconcile(assignment);

            // Now process the callback. It should throw an exception, but it should still finish and allow
            // the reconciler to alter the assigned partition set.
            assertThrows(KafkaException.class, () -> backgroundEventProcessor.process());
            applicationEventProcessor.process();
            assertEquals(newTopicPartitions(0, 2), subscriptions.assignedPartitions());
        }
    }

    private SortedSet<TopicPartition> newTopicPartitions(Integer... partitions) {
        SortedSet<TopicPartition> topicPartitions = new TreeSet<>(new Utils.TopicPartitionComparator());

        if (partitions != null) {
            for (int partition : partitions)
                topicPartitions.add(new TopicPartition(DEFAULT_TOPIC_NAME, partition));
        }

        return topicPartitions;
    }
}
