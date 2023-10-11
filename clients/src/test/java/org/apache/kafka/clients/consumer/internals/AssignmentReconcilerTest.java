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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.AssignmentReconciler.ReconciliationResult;
import org.apache.kafka.clients.consumer.internals.events.AssignPartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.LosePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.RebalanceCallbackEvent;
import org.apache.kafka.clients.consumer.internals.events.RevokePartitionsEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.Assignment;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.TopicPartitions;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.AssignmentReconciler.ReconciliationResult.APPLIED_LOCALLY;
import static org.apache.kafka.clients.consumer.internals.AssignmentReconciler.ReconciliationResult.RECONCILING;
import static org.apache.kafka.clients.consumer.internals.AssignmentReconciler.ReconciliationResult.UNCHANGED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.processRebalanceCallback;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AssignmentReconcilerTest {

    private static final String DEFAULT_TOPIC_NAME = "test-topic";
    private static final Uuid DEFAULT_TOPIC_ID = Uuid.randomUuid();
    private SubscriptionState subscriptions;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private ConsumerRebalanceListenerInvoker callbackInvoker;
    private AssignmentReconciler reconciler;

    @Test
    public void testAssignment() {
        Uuid topicId = DEFAULT_TOPIC_ID;
        String topicName = DEFAULT_TOPIC_NAME;
        setup(Collections.singletonMap(topicName, topicId));

        // Create our initial assignment
        Optional<Assignment> assignment = newAssignment(newTopicPartitions(topicId, 0, 1, 2, 3));

        // Start the reconciliation process. At this point, since there are no partitions assigned to our
        // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
        // prematurely assigned anything to the SubscriptionState and that our result is RECONCILING.
        reconcile(assignment, RECONCILING);
        assertSubscriptionStatePartitionsEquals(Collections.emptySet());

        // This is an intentionally superfluous call. We're just checking that we're in the same state as
        // the last time we called maybeReconcile. Because we haven't executed the ConsumerRebalanceListener,
        // the state of the reconciliation is still in progress.
        reconcile(assignment, RECONCILING);
        assertSubscriptionStatePartitionsEquals(Collections.emptySet());

        // Grab the background event. Because we didn't remove any partitions, but only added them, jump
        // directly to the assign partitions. Let's verify that there's an appropriate event on the
        // background event queue, it has the correct partitions, and the future is there but not complete.
        AssignPartitionsEvent event = pollBackgroundEvent(AssignPartitionsEvent.class);
        assertEventPartitionsEquals(event, newTopicPartitions(topicName, 0, 1, 2, 3));

        // Complete the future to signal to the reconciler that the ConsumerRebalanceListener callback
        // has completed. This will trigger the "commit" of the partition assignment to the SubscriptionState.
        assertSubscriptionStatePartitionsEquals(Collections.emptySet());
        processRebalanceCallback(callbackInvoker, event);
        assertSubscriptionStatePartitionsEquals(newTopicPartitions(topicName, 0, 1, 2, 3));

        // Call the reconciler and verify that it did "commit" the partition assignment as expected.
        reconcile(assignment, APPLIED_LOCALLY);

        // If we ask the reconciler to reconcile a previously reconciled assignment, it should tell us
        // that nothing changed.
        reconcile(assignment, UNCHANGED);
    }

    @Test
    public void testAssignmentAndRevocation() {
        Uuid topicId = DEFAULT_TOPIC_ID;
        String topicName = DEFAULT_TOPIC_NAME;
        setup(Collections.singletonMap(topicName, topicId));

        // Create our initial assignment that adds four partitions
        {
            Optional<Assignment> assignment = newAssignment(newTopicPartitions(topicId, 0, 1, 2, 3));

            // Start the reconciliation process. At this point, since there are no partitions assigned to our
            // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
            // prematurely assigned anything to the SubscriptionState and that our result is RECONCILING.
            reconcile(assignment, RECONCILING);
            assertSubscriptionStatePartitionsEquals(Collections.emptySet());

            // Grab the background event. Because we didn't remove any partitions, but only added them, jump
            // directly to the assign partitions. Let's verify that there's an appropriate event on the
            // background event queue, it has the correct partitions, and the future is there but not complete.
            AssignPartitionsEvent event = pollBackgroundEvent(AssignPartitionsEvent.class);
            assertEventPartitionsEquals(event, newTopicPartitions(topicName, 0, 1, 2, 3));

            // Now process the callback.
            processRebalanceCallback(callbackInvoker, event);
            assertTrue(event.future().isDone());
            assertSubscriptionStatePartitionsEquals(newTopicPartitions(topicName, 0, 1, 2, 3));

            // Call the reconciler and verify that it did "commit" the partition assignment as expected.
            reconcile(assignment, APPLIED_LOCALLY);
        }

        // Create our follow-up assignment that removes two partitions.
        {
            Optional<Assignment> assignment = newAssignment(newTopicPartitions(topicId, 0, 2));

            // Continue the reconciliation process. Since we have partitions assigned, we will need to revoke some
            // old partitions that are no longer part of the target assignment.
            reconcile(assignment, RECONCILING);
            assertSubscriptionStatePartitionsEquals(newTopicPartitions(topicName, 0, 1, 2, 3));

            // Grab the background event. We are removing some partitions, so verify that we have the correct event
            // type on the background event queue, and it has the correct partitions to remove.
            RevokePartitionsEvent event = pollBackgroundEvent(RevokePartitionsEvent.class);
            assertEventPartitionsEquals(event, newTopicPartitions(topicName, 1, 3));

            // Now process the callback.
            processRebalanceCallback(callbackInvoker, event);
            assertTrue(event.future().isDone());
            assertSubscriptionStatePartitionsEquals(newTopicPartitions(topicName, 0, 2));

            // Call the reconciler and verify that it did "commit" the partition assignment as expected.
            reconcile(assignment, APPLIED_LOCALLY);
        }

        // If we ask the reconciler to reconcile a previously reconciled assignment, it should tell us
        // that nothing changed.
        reconcile(newAssignment(newTopicPartitions(topicId, 0, 2)), UNCHANGED);
    }

    @Test
    public void testStuckCallbacks() {
        Uuid topicId = DEFAULT_TOPIC_ID;
        String topicName = DEFAULT_TOPIC_NAME;
        setup(Collections.singletonMap(topicName, topicId));

        Optional<Assignment> assignment = newAssignment(newTopicPartitions(topicId, 0, 1, 2, 3));

        // Start the reconciliation process. At this point, since there are no partitions assigned to our
        // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
        // prematurely assigned anything to the SubscriptionState and that our result is RECONCILING.
        reconcile(assignment, RECONCILING);
        assertSubscriptionStatePartitionsEquals(Collections.emptySet());

        // Grab the background event. Because we didn't remove any partitions, but only added them, jump
        // directly to the assign partitions. Let's verify that there's an appropriate event on the
        // background event queue, it has the correct partitions, and the future is there but not complete.
        AssignPartitionsEvent event = pollBackgroundEvent(AssignPartitionsEvent.class);
        assertEventPartitionsEquals(event, newTopicPartitions(topicName, 0, 1, 2, 3));
        assertFalse(event.future().isDone());

        // Call the reconciler and make sure it thinks that we're still reconciling.
        reconcile(assignment, RECONCILING);
    }

    @Test
    public void testLose() {
        Uuid topicId = DEFAULT_TOPIC_ID;
        String topicName = DEFAULT_TOPIC_NAME;
        setup(Collections.singletonMap(topicName, topicId));

        // This mimics having set up an assignment already.
        SortedSet<TopicPartition> partitions = newTopicPartitions(topicName, 0, 1, 2, 3);
        subscriptions.assignFromSubscribed(partitions);

        assertSubscriptionStatePartitionsEquals(partitions);
        lose(RECONCILING);
        assertSubscriptionStatePartitionsEquals(newTopicPartitions(topicName, 0, 1, 2, 3));

        // Grab the background event. Because we are "losing" the partitions, verify that there's an
        // appropriate event on the background event queue, it still has the partitions, and the future is
        // there but not complete.
        LosePartitionsEvent event = pollBackgroundEvent(LosePartitionsEvent.class);
        assertEventPartitionsEquals(event, partitions);
        assertFalse(event.future().isDone());

        // Now process the callback. Afterward we should have an empty set of partitions
        processRebalanceCallback(callbackInvoker, event);
        assertTrue(event.future().isDone());
        assertSubscriptionStatePartitionsEquals(Collections.emptySet());

        // Call the reconciler and make sure it knows we're done.
        lose(UNCHANGED);
    }

    @Test
    public void testRevocationFailure() {
        ConsumerRebalanceListener failingListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                throw new KafkaException("Test failure");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        };

        Uuid topicId = DEFAULT_TOPIC_ID;
        String topicName = DEFAULT_TOPIC_NAME;
        setup(Collections.singletonMap(topicName, topicId), failingListener);

        // This mimics having set up an assignment of four partitions.
        SortedSet<TopicPartition> partitions = newTopicPartitions(topicName, 0, 1, 2, 3);
        subscriptions.assignFromSubscribed(partitions);

        // When revoking partitions, we get an error. This should not stop the reconciliation process, though.
        {
            Optional<Assignment> assignment = newAssignment(newTopicPartitions(topicId, 0, 2));

            // Start the reconciliation process.
            reconcile(assignment, RECONCILING);

            RevokePartitionsEvent event = pollBackgroundEvent(RevokePartitionsEvent.class);
            assertEventPartitionsEquals(event, newTopicPartitions(topicName, 1, 3));
            assertFalse(event.future().isDone());

            // Now process the callback. It should throw an exception, but it should still finish and allow
            // the reconciler to alter the assigned partition set.
            assertThrows(KafkaException.class, () -> processRebalanceCallback(callbackInvoker, event));
            assertTrue(event.future().isDone());
            assertTrue(event.future().isCompletedExceptionally());
            assertSubscriptionStatePartitionsEquals(newTopicPartitions(topicName, 0, 2));
        }
    }

    void reconcile(Optional<Assignment> assignment, ReconciliationResult expected) {
        ReconciliationResult result = reconciler.maybeReconcile(assignment);
        assertEquals(expected, result);
    }

    void lose(ReconciliationResult expected) {
        ReconciliationResult result = reconciler.lose();
        assertEquals(expected, result);
    }

    private void assertSubscriptionStatePartitionsEquals(Set<TopicPartition> expected) {
        assertEquals(expected, subscriptions.assignedPartitions());
    }

    private void assertEventPartitionsEquals(RebalanceCallbackEvent event, SortedSet<TopicPartition> expected) {
        assertEquals(expected, event.partitions());
    }

    private TopicPartitions newTopicPartitions(Uuid topicId, Integer... partitions) {
        TopicPartitions topicPartitions = new TopicPartitions();

        if (topicId != null)
            topicPartitions.setTopicId(topicId);

        if (partitions != null)
            topicPartitions.setPartitions(Arrays.asList(partitions));

        return topicPartitions;
    }

    private SortedSet<TopicPartition> newTopicPartitions(String topicName, Integer... partitions) {
        SortedSet<TopicPartition> topicPartitions = new TreeSet<>(new Utils.TopicPartitionComparator());

        if (partitions != null) {
            for (int partition : partitions)
                topicPartitions.add(new TopicPartition(topicName, partition));
        }

        return topicPartitions;
    }

    private Optional<Assignment> newAssignment(TopicPartitions... partitions) {
        List<TopicPartitions> topicPartitions = new ArrayList<>();

        if (partitions != null) {
            Collections.addAll(topicPartitions, partitions);
        }

        Assignment assignment = new Assignment();
        assignment.setAssignedTopicPartitions(topicPartitions);
        return Optional.of(assignment);
    }

    private void setup(Map<String, Uuid> topics) {
        setup(topics, new NoOpConsumerRebalanceListener());
    }

    private void setup(Map<String, Uuid> topics, ConsumerRebalanceListener listener) {
        Time time = new MockTime();
        LogContext logContext = new LogContext();

        // Create our SubscriptionState and subscribe to the topics.
        subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        subscriptions.subscribe(topics.keySet(), listener);

        // Create our metadata and ensure at has our topic name to topic ID mapping.
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWithIds(
                "dummy",
                1,
                Collections.emptyMap(),
                topics.keySet().stream().collect(Collectors.toMap(t -> t, t -> 3)),
                tp -> 0,
                topics
        );
        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerMetadata metadata = new ConsumerMetadata(
                config,
                subscriptions,
                logContext,
                new ClusterResourceListeners()
        );
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // We need the background event queue to check for the events from the network thread to the application thread
        // to signal the ConsumerRebalanceListener callbacks.
        backgroundEventQueue = new LinkedBlockingQueue<>();

        callbackInvoker = new ConsumerRebalanceListenerInvoker(
                logContext,
                subscriptions,
                time,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
        );

        reconciler = new AssignmentReconciler(logContext, subscriptions, metadata, backgroundEventQueue);
    }

    private <T extends RebalanceCallbackEvent> T pollBackgroundEvent(Class<T> expectedEventType) {
        BackgroundEvent e = backgroundEventQueue.poll();
        assertNotNull(e);
        assertEquals(e.getClass(), expectedEventType);
        T event = expectedEventType.cast(e);
        CompletableFuture<Void> future = event.future();
        assertNotNull(future);
        assertFalse(future.isDone());
        return event;
    }
}
