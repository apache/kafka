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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class encapsulates the invocation of the callback methods defined in the {@link ConsumerRebalanceListener}
 * interface. When consumer group partition assignment changes, these methods are invoked. This class wraps those
 * callback calls with some logging, optional {@link Sensor} updates, etc.
 */
public class ConsumerRebalanceListenerInvoker {

    private final Logger log;
    private final SubscriptionState subscriptions;
    private final Time time;
    private final ConsumerCoordinatorMetrics sensors;

    ConsumerRebalanceListenerInvoker(LogContext logContext,
                                     SubscriptionState subscriptions,
                                     Time time,
                                     ConsumerCoordinatorMetrics sensors) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.time = time;
        this.sensors = sensors;
    }

    Exception invokePartitionsAssigned(final SortedSet<TopicPartition> assignedPartitions) {
        log.info("Adding newly assigned partitions: {}", Utils.join(assignedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            listener.onPartitionsAssigned(assignedPartitions);
            sensors.assignCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsAssigned for partitions {}",
                    listener.getClass().getName(), assignedPartitions, e);
            return e;
        }

        return null;
    }

    Exception invokePartitionsRevoked(final SortedSet<TopicPartition> revokedPartitions) {
        log.info("Revoke previously assigned partitions {}", Utils.join(revokedPartitions, ", "));
        Set<TopicPartition> revokePausedPartitions = subscriptions.pausedPartitions();
        revokePausedPartitions.retainAll(revokedPartitions);
        if (!revokePausedPartitions.isEmpty())
            log.info("The pause flag in partitions [{}] will be removed due to revocation.", org.apache.kafka.common.utils.Utils.join(revokePausedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            listener.onPartitionsRevoked(revokedPartitions);
            sensors.revokeCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsRevoked for partitions {}",
                    listener.getClass().getName(), revokedPartitions, e);
            return e;
        }

        return null;
    }

    Exception invokePartitionsLost(final SortedSet<TopicPartition> lostPartitions) {
        log.info("Lost previously assigned partitions {}", Utils.join(lostPartitions, ", "));
        Set<TopicPartition> lostPausedPartitions = subscriptions.pausedPartitions();
        lostPausedPartitions.retainAll(lostPartitions);
        if (!lostPausedPartitions.isEmpty())
            log.info("The pause flag in partitions [{}] will be removed due to partition lost.", Utils.join(lostPausedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            listener.onPartitionsLost(lostPartitions);
            sensors.loseCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsLost for partitions {}",
                    listener.getClass().getName(), lostPartitions, e);
            return e;
        }

        return null;
    }

    public void rebalance(final SortedSet<TopicPartition> revokedPartitions,
                          final SortedSet<TopicPartition> assignedPartitions) {
        final AtomicReference<Exception> firstException = new AtomicReference<>(null);

        if (!revokedPartitions.isEmpty()) {
            // Revoke partitions that were previously owned but no longer assigned;
            // note that we should only change the assignment (or update the assignor's state)
            // AFTER we've triggered the revoke callback
            firstException.compareAndSet(null, invokePartitionsRevoked(revokedPartitions));
        }

        if (!assignedPartitions.isEmpty()) {
            // Add partitions that were not previously owned but are now assigned
            firstException.compareAndSet(null, invokePartitionsAssigned(assignedPartitions));
        }

        if (firstException.get() == null)
            return;

        if (firstException.get() instanceof KafkaException)
            throw (KafkaException) firstException.get();
        else
            throw new KafkaException("User rebalance callback throws an error", firstException.get());
    }

    public void lose(final SortedSet<TopicPartition> lostPartitions) {
        if (lostPartitions.isEmpty())
            return;

        log.info("Giving away all assigned partitions as lost since generation/memberID has been reset, " +
                 "indicating that consumer is in old state or no longer part of the group");
        Exception e = invokePartitionsLost(lostPartitions);

        if (e == null)
            return;

        if (e instanceof KafkaException)
            throw (KafkaException) e;
        else
            throw new KafkaException("User rebalance callback throws an error", e);
    }
}
