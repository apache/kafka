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
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.PartitionAssignmentChangedCallbacksInvokedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionAssignmentChangedStartedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionAssignmentLostCallbackInvokedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionAssignmentLostStartedEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.Assignment;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.TopicPartitions;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.processRebalanceCallback;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentReconcilerTest {

    private static final String DEFAULT_TOPIC_NAME = "test-topic";
    private static final Uuid DEFAULT_TOPIC_ID = Uuid.randomUuid();
    private SubscriptionState subscriptions;
    private BlockingQueue<ApplicationEvent> applicationEventQueue;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private ConsumerRebalanceListenerInvoker callbackInvoker;
    private AssignmentReconciler reconciler;
    private EventHandler eventHandler;

    @Test
    public void testAssignment() {
        Uuid topicId = DEFAULT_TOPIC_ID;
        String topicName = DEFAULT_TOPIC_NAME;
        setup(Collections.singletonMap(topicName, topicId));

        // Create our initial assignment
        Assignment assignment = newAssignment(newTopicPartitions(topicId, 0, 1, 2, 3));

        // Start the reconciliation process. At this point, since there are no partitions assigned to our
        // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
        // prematurely assigned anything to the subscriptions.
        reconciler.reconcile(assignment);
        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());

        // Grab the background event. Because we didn't remove any partitions, but only added them, jump
        // directly to the assign partitions. Let's verify that there's an appropriate event on the
        // background event queue, and it has the correct partitions.
        PartitionAssignmentChangedStartedEvent event = pollBackgroundEvent();
        assertEquals(event.assignedPartitions(), newTopicPartitions(topicName, 0, 1, 2, 3));

        // Complete the future to signal to the reconciler that the ConsumerRebalanceListener callback
        // has completed. This will trigger the "commit" of the partition assignment to the subscriptions.
        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());
        processRebalanceCallback(eventHandler, callbackInvoker, event);
        PartitionAssignmentChangedCallbacksInvokedEvent invokedEvent = pollApplicationEvent();
        assertEquals(invokedEvent.assignedPartitions(), newTopicPartitions(topicName, 0, 1, 2, 3));
        assertEquals(Optional.empty(), invokedEvent.error());
        reconciler.reconciliationCallbacksInvoked(invokedEvent.revokedPartitions(), invokedEvent.assignedPartitions());
        assertEquals(newTopicPartitions(topicName, 0, 1, 2, 3), subscriptions.assignedPartitions());
    }

    @Test
    public void testAssignmentAndRevocation() {
        Uuid topicId = DEFAULT_TOPIC_ID;
        String topicName = DEFAULT_TOPIC_NAME;
        setup(Collections.singletonMap(topicName, topicId));

        // Create our initial assignment that adds four partitions
        {
            Assignment assignment = newAssignment(newTopicPartitions(topicId, 0, 1, 2, 3));

            // Start the reconciliation process. At this point, since there are no partitions assigned to our
            // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
            // prematurely assigned anything to the subscriptions.
            reconciler.reconcile(assignment);
            assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());

            // Grab the background event. Because we didn't remove any partitions, but only added them, jump
            // directly to the assign partitions. Let's verify that there's an appropriate event on the
            // background event queue, and it has the correct partitions.
            PartitionAssignmentChangedStartedEvent event = pollBackgroundEvent();
            assertEquals(event.assignedPartitions(), newTopicPartitions(topicName, 0, 1, 2, 3));

            // Now process the callback.
            processRebalanceCallback(eventHandler, callbackInvoker, event);
            PartitionAssignmentChangedCallbacksInvokedEvent invokedEvent = pollApplicationEvent();
            assertEquals(invokedEvent.assignedPartitions(), newTopicPartitions(topicName, 0, 1, 2, 3));
            assertEquals(Optional.empty(), invokedEvent.error());
            reconciler.reconciliationCallbacksInvoked(invokedEvent.revokedPartitions(), invokedEvent.assignedPartitions());
            assertEquals(newTopicPartitions(topicName, 0, 1, 2, 3), subscriptions.assignedPartitions());
        }

        // Create our follow-up assignment that removes two partitions.
        {
            Assignment assignment = newAssignment(newTopicPartitions(topicId, 0, 2));

            // We get another assignment. Since we have partitions assigned, we will need to revoke some
            // old partitions that are no longer part of the new target assignment.
            reconciler.reconcile(assignment);
            assertEquals(newTopicPartitions(topicName, 0, 1, 2, 3), subscriptions.assignedPartitions());

            // Grab the background event. We are removing some partitions, so verify that we have the correct event
            // type on the background event queue, and it has the correct partitions to remove.
            PartitionAssignmentChangedStartedEvent event = pollBackgroundEvent();
            assertEquals(event.revokedPartitions(), newTopicPartitions(topicName, 1, 3));

            // Now process the callback.
            processRebalanceCallback(eventHandler, callbackInvoker, event);
            PartitionAssignmentChangedCallbacksInvokedEvent invokedEvent = pollApplicationEvent();
            assertEquals(invokedEvent.revokedPartitions(), newTopicPartitions(topicName, 1, 3));
            reconciler.reconciliationCallbacksInvoked(invokedEvent.revokedPartitions(), invokedEvent.assignedPartitions());
            assertEquals(newTopicPartitions(topicName, 0, 2), subscriptions.assignedPartitions());
        }
    }

    @Test
    public void testLose() {
        String topicName = DEFAULT_TOPIC_NAME;
        setup(Collections.singletonMap(topicName, DEFAULT_TOPIC_ID));

        // This mimics having set up an assignment already.
        SortedSet<TopicPartition> partitions = newTopicPartitions(topicName, 0, 1, 2, 3);
        subscriptions.assignFromSubscribed(partitions);

        assertEquals(partitions, subscriptions.assignedPartitions());
        reconciler.lost();
        assertEquals(partitions, subscriptions.assignedPartitions());

        // Grab the background event. Because we are "losing" the partitions, verify that there's an
        // appropriate event on the background event queue, and it still has the partitions.
        PartitionAssignmentLostStartedEvent event = pollBackgroundEvent();
        assertEquals(partitions, event.lostPartitions());

        // Now process the callback. Afterward we should have an empty set of partitions
        processRebalanceCallback(eventHandler, callbackInvoker, event);
        PartitionAssignmentLostCallbackInvokedEvent invokedEvent = pollApplicationEvent();
        assertEquals(partitions, invokedEvent.lostPartitions());
        reconciler.lostCallbackInvoked();
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

        Uuid topicId = DEFAULT_TOPIC_ID;
        String topicName = DEFAULT_TOPIC_NAME;
        setup(Collections.singletonMap(topicName, topicId), failingListener);

        // This mimics having set up an assignment of four partitions.
        SortedSet<TopicPartition> partitions = newTopicPartitions(topicName, 0, 1, 2, 3);
        subscriptions.assignFromSubscribed(partitions);

        // When revoking partitions, we get an error. This should not stop the reconciliation process, though.
        {
            Assignment assignment = newAssignment(newTopicPartitions(topicId, 0, 2));

            // Start the reconciliation process.
            reconciler.reconcile(assignment);

            PartitionAssignmentChangedStartedEvent event = pollBackgroundEvent();
            assertEquals(event.revokedPartitions(), newTopicPartitions(topicName, 1, 3));

            // Now process the callback. It should throw an exception, but it should still finish and allow
            // the reconciler to alter the assigned partition set.
            assertThrows(KafkaException.class, () -> processRebalanceCallback(eventHandler, callbackInvoker, event));
            PartitionAssignmentChangedCallbacksInvokedEvent invokedEvent = pollApplicationEvent();
            assertEquals(invokedEvent.revokedPartitions(), newTopicPartitions(topicName, 1, 3));
            reconciler.reconciliationCallbacksInvoked(invokedEvent.revokedPartitions(), invokedEvent.assignedPartitions());
            assertEquals(newTopicPartitions(topicName, 0, 2), subscriptions.assignedPartitions());
        }
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

    private Assignment newAssignment(TopicPartitions... partitions) {
        List<TopicPartitions> topicPartitions = new ArrayList<>();

        if (partitions != null) {
            Collections.addAll(topicPartitions, partitions);
        }

        Assignment assignment = new Assignment();
        assignment.setTopicPartitions(topicPartitions);
        return assignment;
    }

    private void setup(Map<String, Uuid> topics) {
        setup(topics, new NoOpConsumerRebalanceListener());
    }

    private void setup(Map<String, Uuid> topics, ConsumerRebalanceListener listener) {
        Time time = new MockTime();
        LogContext logContext = new LogContext();

        // Create our subscriptions and subscribe to the topics.
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
        applicationEventQueue = new LinkedBlockingQueue<>();
        backgroundEventQueue = new LinkedBlockingQueue<>();

        ConsumerCoordinatorMetrics sensors = new ConsumerCoordinatorMetrics(
                subscriptions,
                new Metrics(),
                CONSUMER_METRIC_GROUP_PREFIX
        );
        callbackInvoker = new ConsumerRebalanceListenerInvoker(
                logContext,
                subscriptions,
                time,
                sensors
        );

        reconciler = new AssignmentReconciler(logContext, subscriptions, metadata, backgroundEventQueue);
        eventHandler = new EventHandler() {
            @Override
            public Optional<BackgroundEvent> poll() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isEmpty() {
                return applicationEventQueue.isEmpty();
            }

            @Override
            public boolean add(ApplicationEvent event) {
                return applicationEventQueue.add(event);
            }

            @Override
            public <T> T addAndGet(CompletableApplicationEvent<T> event, Timer timer) {
                applicationEventQueue.add(event);
                return event.get(timer);
            }

            @Override
            public void close() throws IOException {
                // No op
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <T extends ApplicationEvent> T pollApplicationEvent() {
        ApplicationEvent event = applicationEventQueue.poll();
        assertNotNull(event);
        return (T) event;
    }

    @SuppressWarnings("unchecked")
    private <T extends BackgroundEvent> T pollBackgroundEvent() {
        BackgroundEvent event = backgroundEventQueue.poll();
        assertNotNull(event);
        return (T) event;
    }
}
