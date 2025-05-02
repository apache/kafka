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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.server.util.ShutdownableThread;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConsumerAssignmentPoller extends ShutdownableThread {
    private final Consumer<byte[], byte[]> consumer;
    private final Set<TopicPartition> partitionsToAssign;
    private final ConsumerRebalanceListener userRebalanceListener;

    private volatile Throwable thrownException = null;
    private volatile int receivedMessages = 0;

    private final Set<TopicPartition> partitionAssignment = new HashSet<>();
    private volatile boolean subscriptionChanged = false;
    private List<String> topicsSubscription;
    private final ConsumerRebalanceListener rebalanceListener;

    public ConsumerAssignmentPoller(
        Consumer<byte[], byte[]> consumer,
        List<String> topicsToSubscribe
    ) {
        this(consumer, topicsToSubscribe, Set.of(), null);
    }

    public ConsumerAssignmentPoller(
        Consumer<byte[], byte[]> consumer,
        List<String> topicsToSubscribe,
        Set<TopicPartition> partitionsToAssign,
        ConsumerRebalanceListener userRebalanceListener
    ) {
        super("daemon-consumer-assignment", false);
        this.consumer = consumer;
        this.topicsSubscription = topicsToSubscribe;
        this.partitionsToAssign = partitionsToAssign;
        this.userRebalanceListener = userRebalanceListener;

        this.rebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitionAssignment.addAll(partitions);
                if (userRebalanceListener != null)
                    userRebalanceListener.onPartitionsAssigned(partitions);
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                partitionAssignment.removeAll(partitions);
                if (userRebalanceListener != null)
                    userRebalanceListener.onPartitionsRevoked(partitions);
            }
        };

        if (partitionsToAssign.isEmpty()) {
            consumer.subscribe(topicsToSubscribe, rebalanceListener);
        } else {
            consumer.assign(List.copyOf(partitionsToAssign));
        }
    }

    public Set<TopicPartition> consumerAssignment() {
        return Set.copyOf(partitionAssignment);
    }

    /**
     * Subscribe consumer to a new set of topics.
     * Since this method is most likely to be called from a different thread, this function
     * just "schedules" the subscription change, and actual call to {@link org.apache.kafka.clients.consumer.Consumer#subscribe(Collection)} is done
     * in the doWork() method
     * <p>
     * This method does not allow changing the subscription until doWork processes the previous call
     * to this method. This is just to avoid race conditions and provide enough functionality for testing purposes
     *
     * @param newTopicsToSubscribe new topics to subscribe to
     */
    public void subscribe(List<String> newTopicsToSubscribe) {
        if (subscriptionChanged)
            throw new IllegalStateException("Do not call subscribe until the previous subscribe request is processed.");
        if (!partitionsToAssign.isEmpty())
            throw new IllegalStateException("Cannot call subscribe when configured to use manual partition assignment");

        topicsSubscription = newTopicsToSubscribe;
        subscriptionChanged = true;
    }

    @Override
    public boolean initiateShutdown() {
        boolean res = super.initiateShutdown();
        consumer.wakeup();
        return res;
    }

    @Override
    public void doWork() {
        if (subscriptionChanged) {
            consumer.subscribe(topicsSubscription, rebalanceListener);
            subscriptionChanged = false;
        }
        try {
            receivedMessages += consumer.poll(Duration.ofMillis(50)).count();
        } catch (WakeupException e) {
            // ignore for shutdown
        } catch (Throwable e) {
            thrownException = e;
            throw e;
        }
    }
}
