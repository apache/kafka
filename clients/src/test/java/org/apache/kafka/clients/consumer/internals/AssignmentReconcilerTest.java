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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionLostCompleteEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionLostStartedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionReconciliationCompleteEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionReconciliationStartedEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.processRebalanceCallback;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentReconcilerTest {
//
//    private static final String DEFAULT_TOPIC_NAME = "test-topic";
//    private SubscriptionState subscriptions;
//    private BlockingQueue<ApplicationEvent> applicationEventQueue;
//    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
//    private ConsumerRebalanceListenerInvoker callbackInvoker;
//    private AssignmentReconciler reconciler;
//    private EventHandler eventHandler;
//
//    @Test
//    public void testAssignment() {
//        setup();
//
//        // Create our initial assignment
//        Set<TopicPartition> assignment = newTopicPartitions(0, 1, 2, 3);
//
//        // Start the reconciliation process. At this point, since there are no partitions assigned to our
//        // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
//        // prematurely assigned anything to the subscriptions.
//        reconciler.startReconcile(assignment);
//        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());
//
//        // Grab the background event. Because we didn't remove any partitions, but only added them, jump
//        // directly to the assign partitions. Let's verify that there's an appropriate event on the
//        // background event queue, and it has the correct partitions.
//        PartitionReconciliationStartedEvent event = pollBackgroundEvent();
//        assertEquals(event.assignedPartitions(), newTopicPartitions(0, 1, 2, 3));
//
//        // Complete the future to signal to the reconciler that the ConsumerRebalanceListener callback
//        // has completed. This will trigger the "commit" of the partition assignment to the subscriptions.
//        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());
//        processRebalanceCallback(eventHandler, callbackInvoker, event);
//        PartitionReconciliationCompleteEvent invokedEvent = pollApplicationEvent();
//        assertEquals(invokedEvent.assignedPartitions(), newTopicPartitions(0, 1, 2, 3));
//        assertEquals(Optional.empty(), invokedEvent.error());
//        reconciler.completeReconcile(invokedEvent.revokedPartitions(), invokedEvent.assignedPartitions());
//        assertEquals(newTopicPartitions(0, 1, 2, 3), subscriptions.assignedPartitions());
//    }
//
//    @Test
//    public void testAssignmentAndRevocation() {
//        setup();
//
//        // Create our initial assignment that adds four partitions
//        {
//            Set<TopicPartition> assignment = newTopicPartitions(0, 1, 2, 3);
//
//            // Start the reconciliation process. At this point, since there are no partitions assigned to our
//            // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
//            // prematurely assigned anything to the subscriptions.
//            reconciler.startReconcile(assignment);
//            assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());
//
//            // Grab the background event. Because we didn't remove any partitions, but only added them, jump
//            // directly to the assign partitions. Let's verify that there's an appropriate event on the
//            // background event queue, and it has the correct partitions.
//            PartitionReconciliationStartedEvent event = pollBackgroundEvent();
//            assertEquals(event.assignedPartitions(), newTopicPartitions(0, 1, 2, 3));
//
//            // Now process the callback.
//            processRebalanceCallback(eventHandler, callbackInvoker, event);
//            PartitionReconciliationCompleteEvent invokedEvent = pollApplicationEvent();
//            assertEquals(invokedEvent.assignedPartitions(), newTopicPartitions(0, 1, 2, 3));
//            assertEquals(Optional.empty(), invokedEvent.error());
//            reconciler.completeReconcile(invokedEvent.revokedPartitions(), invokedEvent.assignedPartitions());
//            assertEquals(newTopicPartitions(0, 1, 2, 3), subscriptions.assignedPartitions());
//        }
//
//        // Create our follow-up assignment that removes two partitions.
//        {
//            Set<TopicPartition> assignment = newTopicPartitions(0, 2);
//
//            // We get another assignment. Since we have partitions assigned, we will need to revoke some
//            // old partitions that are no longer part of the new target assignment.
//            reconciler.startReconcile(assignment);
//            assertEquals(newTopicPartitions(0, 1, 2, 3), subscriptions.assignedPartitions());
//
//            // Grab the background event. We are removing some partitions, so verify that we have the correct event
//            // type on the background event queue, and it has the correct partitions to remove.
//            PartitionReconciliationStartedEvent event = pollBackgroundEvent();
//            assertEquals(event.revokedPartitions(), newTopicPartitions(1, 3));
//
//            // Now process the callback.
//            processRebalanceCallback(eventHandler, callbackInvoker, event);
//            PartitionReconciliationCompleteEvent invokedEvent = pollApplicationEvent();
//            assertEquals(invokedEvent.revokedPartitions(), newTopicPartitions(1, 3));
//            reconciler.completeReconcile(invokedEvent.revokedPartitions(), invokedEvent.assignedPartitions());
//            assertEquals(newTopicPartitions(0, 2), subscriptions.assignedPartitions());
//        }
//    }
//
//    @Test
//    public void testLose() {
//        setup();
//
//        // This mimics having set up an assignment already.
//        SortedSet<TopicPartition> partitions = newTopicPartitions(0, 1, 2, 3);
//        subscriptions.assignFromSubscribed(partitions);
//
//        assertEquals(partitions, subscriptions.assignedPartitions());
//        reconciler.startLost();
//        assertEquals(partitions, subscriptions.assignedPartitions());
//
//        // Grab the background event. Because we are "losing" the partitions, verify that there's an
//        // appropriate event on the background event queue, and it still has the partitions.
//        PartitionLostStartedEvent event = pollBackgroundEvent();
//        assertEquals(partitions, event.lostPartitions());
//
//        // Now process the callback. Afterward we should have an empty set of partitions
//        processRebalanceCallback(eventHandler, callbackInvoker, event);
//        PartitionLostCompleteEvent invokedEvent = pollApplicationEvent();
//        assertEquals(partitions, invokedEvent.lostPartitions());
//        reconciler.completeLost(invokedEvent.lostPartitions());
//        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());
//    }
//
//    @Test
//    public void testRevocationFailure() {
//        ConsumerRebalanceListener failingListener = new ConsumerRebalanceListener() {
//            @Override
//            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//                throw new KafkaException("Simulating callback failure");
//            }
//
//            @Override
//            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//
//            }
//        };
//
//        setup(failingListener);
//
//        // This mimics having set up an assignment of four partitions.
//        SortedSet<TopicPartition> partitions = newTopicPartitions(0, 1, 2, 3);
//        subscriptions.assignFromSubscribed(partitions);
//
//        // When revoking partitions, we get an error. This should not stop the reconciliation process, though.
//        {
//            Set<TopicPartition> assignment = newTopicPartitions(0, 2);
//
//            // Start the reconciliation process.
//            reconciler.startReconcile(assignment);
//
//            PartitionReconciliationStartedEvent event = pollBackgroundEvent();
//            assertEquals(event.revokedPartitions(), newTopicPartitions(1, 3));
//
//            // Now process the callback. It should throw an exception, but it should still finish and allow
//            // the reconciler to alter the assigned partition set.
//            assertThrows(KafkaException.class, () -> processRebalanceCallback(eventHandler, callbackInvoker, event));
//            PartitionReconciliationCompleteEvent invokedEvent = pollApplicationEvent();
//            assertEquals(invokedEvent.revokedPartitions(), newTopicPartitions(1, 3));
//            reconciler.completeReconcile(invokedEvent.revokedPartitions(), invokedEvent.assignedPartitions());
//            assertEquals(newTopicPartitions(0, 2), subscriptions.assignedPartitions());
//        }
//    }
//
//    private SortedSet<TopicPartition> newTopicPartitions(Integer... partitions) {
//        SortedSet<TopicPartition> topicPartitions = new TreeSet<>(new Utils.TopicPartitionComparator());
//
//        if (partitions != null) {
//            for (int partition : partitions)
//                topicPartitions.add(new TopicPartition(DEFAULT_TOPIC_NAME, partition));
//        }
//
//        return topicPartitions;
//    }
//
//    private void setup() {
//        setup(new NoOpConsumerRebalanceListener());
//    }
//
//    private void setup(ConsumerRebalanceListener listener) {
//        Time time = new MockTime();
//        LogContext logContext = new LogContext();
//
//        // Create our subscriptions and subscribe to the topics.
//        subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
//        subscriptions.subscribe(Collections.singleton(DEFAULT_TOPIC_NAME), listener);
//
//        // We need the background event queue to check for the events from the network thread to the application thread
//        // to signal the ConsumerRebalanceListener callbacks.
//        applicationEventQueue = new LinkedBlockingQueue<>();
//        backgroundEventQueue = new LinkedBlockingQueue<>();
//
//        ConsumerCoordinatorMetrics sensors = new ConsumerCoordinatorMetrics(
//                subscriptions,
//                new Metrics(),
//                CONSUMER_METRIC_GROUP_PREFIX
//        );
//        callbackInvoker = new ConsumerRebalanceListenerInvoker(
//                logContext,
//                subscriptions,
//                time,
//                sensors
//        );
//
//        reconciler = new AssignmentReconciler(logContext, subscriptions, backgroundEventQueue);
//        eventHandler = new EventHandler() {
//            @Override
//            public Optional<BackgroundEvent> poll() {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public boolean isEmpty() {
//                return applicationEventQueue.isEmpty();
//            }
//
//            @Override
//            public boolean add(ApplicationEvent event) {
//                return applicationEventQueue.add(event);
//            }
//
//            @Override
//            public <T> T addAndGet(CompletableApplicationEvent<T> event, Timer timer) {
//                applicationEventQueue.add(event);
//                return event.get(timer);
//            }
//
//            @Override
//            public void close() {
//                // No op
//            }
//        };
//    }
//
//    @SuppressWarnings("unchecked")
//    private <T extends ApplicationEvent> T pollApplicationEvent() {
//        ApplicationEvent event = applicationEventQueue.poll();
//        assertNotNull(event);
//        return (T) event;
//    }
//
//    @SuppressWarnings("unchecked")
//    private <T extends BackgroundEvent> T pollBackgroundEvent() {
//        BackgroundEvent event = backgroundEventQueue.poll();
//        assertNotNull(event);
//        return (T) event;
//    }
}
