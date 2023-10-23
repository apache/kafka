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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

/**
 * This class encapsulates the invocation of the callback methods defined in the {@link ConsumerRebalanceListener}
 * interface. When consumer group partition assignment changes, these methods are invoked. This class wraps those
 * callback calls with some logging, optional {@link Sensor} updates, etc.
 */
class ConsumerRebalanceListenerInvoker {

    private final Logger log;
    private final SubscriptionState subscriptions;
    private final Time time;
    private final ConsumerCoordinatorMetrics coordinatorMetrics;

    ConsumerRebalanceListenerInvoker(LogContext logContext,
                                     SubscriptionState subscriptions,
                                     Time time,
                                     ConsumerCoordinatorMetrics coordinatorMetrics) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.time = time;
        this.coordinatorMetrics = coordinatorMetrics;
    }

    Exception invokePartitionsAssigned(final SortedSet<TopicPartition> assignedPartitions) {
        log.info("Adding newly assigned partitions: {}", Utils.join(assignedPartitions, ", "));

        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();

        if (listener.isPresent()) {
            try {
                final long startMs = time.milliseconds();
                listener.get().onPartitionsAssigned(assignedPartitions);
                coordinatorMetrics.assignCallbackSensor.record(time.milliseconds() - startMs);
            } catch (WakeupException | InterruptException e) {
                throw e;
            } catch (Exception e) {
                log.error("User provided listener {} failed on invocation of onPartitionsAssigned for partitions {}",
                        listener.getClass().getName(), assignedPartitions, e);
                return e;
            }
        }

        return null;
    }

    Exception invokePartitionsRevoked(final SortedSet<TopicPartition> revokedPartitions) {
        log.info("Revoke previously assigned partitions {}", Utils.join(revokedPartitions, ", "));
        Set<TopicPartition> revokePausedPartitions = subscriptions.pausedPartitions();
        revokePausedPartitions.retainAll(revokedPartitions);
        if (!revokePausedPartitions.isEmpty())
            log.info("The pause flag in partitions [{}] will be removed due to revocation.", Utils.join(revokePausedPartitions, ", "));

        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();

        if (listener.isPresent()) {
            try {
                final long startMs = time.milliseconds();
                listener.get().onPartitionsRevoked(revokedPartitions);
                coordinatorMetrics.revokeCallbackSensor.record(time.milliseconds() - startMs);
            } catch (WakeupException | InterruptException e) {
                throw e;
            } catch (Exception e) {
                log.error("User provided listener {} failed on invocation of onPartitionsRevoked for partitions {}",
                        listener.getClass().getName(), revokedPartitions, e);
                return e;
            }
        }

        return null;
    }

    Exception invokePartitionsLost(final SortedSet<TopicPartition> lostPartitions) {
        log.info("Lost previously assigned partitions {}", Utils.join(lostPartitions, ", "));
        Set<TopicPartition> lostPausedPartitions = subscriptions.pausedPartitions();
        lostPausedPartitions.retainAll(lostPartitions);
        if (!lostPausedPartitions.isEmpty())
            log.info("The pause flag in partitions [{}] will be removed due to partition lost.", Utils.join(lostPausedPartitions, ", "));

        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();

        if (listener.isPresent()) {
            try {
                final long startMs = time.milliseconds();
                listener.get().onPartitionsLost(lostPartitions);
                coordinatorMetrics.loseCallbackSensor.record(time.milliseconds() - startMs);
            } catch (WakeupException | InterruptException e) {
                throw e;
            } catch (Exception e) {
                log.error("User provided listener {} failed on invocation of onPartitionsLost for partitions {}",
                        listener.getClass().getName(), lostPartitions, e);
                return e;
            }
        }

        return null;
    }
}
