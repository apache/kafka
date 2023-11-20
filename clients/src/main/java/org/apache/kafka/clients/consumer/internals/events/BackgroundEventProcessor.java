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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerInvoker;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link EventProcessor} that is created and executes in the application thread for the purpose of processing
 * {@link BackgroundEvent background events} generated by the {@link ConsumerNetworkThread network thread}.
 * Those events are generally of two types:
 *
 * <ul>
 *     <li>Errors that occur in the network thread that need to be propagated to the application thread</li>
 *     <li>{@link ConsumerRebalanceListener} callbacks that are to be executed on the application thread</li>
 * </ul>
 */
public class BackgroundEventProcessor extends EventProcessor<BackgroundEvent> {

    private final Logger log;
    private final ApplicationEventHandler applicationEventHandler;
    private final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker;

    public BackgroundEventProcessor(final LogContext logContext,
                                    final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                    final ApplicationEventHandler applicationEventHandler,
                                    final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker) {
        super(logContext, backgroundEventQueue);
        this.log = logContext.logger(BackgroundEventProcessor.class);
        this.applicationEventHandler = applicationEventHandler;
        this.rebalanceListenerInvoker = rebalanceListenerInvoker;
    }

    /**
     * Process the events—if any—that were produced by the {@link ConsumerNetworkThread network thread}.
     * It is possible that {@link ErrorBackgroundEvent an error} could occur when processing the events.
     * In such cases, the processor will take a reference to the first error, continue to process the
     * remaining events, and then throw the first error that occurred.
     */
    public void process() {
        AtomicReference<KafkaException> firstError = new AtomicReference<>();

        ProcessHandler<BackgroundEvent> processHandler = (event, error) -> {
            if (error.isPresent()) {
                KafkaException e = error.get();

                if (!firstError.compareAndSet(null, e)) {
                    log.warn("An error occurred when processing the event: {}", e.getMessage(), e);
                }
            }
        };

        process(processHandler);

        if (firstError.get() != null)
            throw firstError.get();
    }

    @Override
    public void process(final BackgroundEvent event) {
        switch (event.type()) {
            case ERROR:
                process((ErrorBackgroundEvent) event);
                return;

            case PARTITION_RECONCILIATION_STARTED:
                process((RebalanceStartedEvent) event);
                return;

            case PARTITION_LOST_STARTED:
                process((PartitionLostStartedEvent) event);
                return;

            default:
                throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");
        }
    }

    @Override
    protected Class<BackgroundEvent> getEventClass() {
        return BackgroundEvent.class;
    }

    private void process(final ErrorBackgroundEvent event) {
        throw event.error();
    }

    private void process(final RebalanceStartedEvent event) {
        final SortedSet<TopicPartition> revokedPartitions = event.revokedPartitions();
        final SortedSet<TopicPartition> assignedPartitions = event.assignedPartitions();
        Optional<KafkaException> error = Optional.empty();

        try {
            rebalanceListenerInvoker.rebalance(revokedPartitions, assignedPartitions);
        } catch (KafkaException e) {
            error = Optional.of(e);
            throw e;
        } finally {
            ApplicationEvent invokedEvent = new RebalanceCompleteEvent(
                    revokedPartitions,
                    assignedPartitions,
                    error);
            applicationEventHandler.add(invokedEvent);
        }
    }

    private void process(final PartitionLostStartedEvent event) {
        final SortedSet<TopicPartition> lostPartitions = event.lostPartitions();
        Optional<KafkaException> error = Optional.empty();

        try {
            rebalanceListenerInvoker.lose(lostPartitions);
        } catch (KafkaException e) {
            error = Optional.of(e);
            throw e;
        } finally {
            ApplicationEvent invokedEvent = new PartitionLostCompleteEvent(lostPartitions, error);
            applicationEventHandler.add(invokedEvent);
        }
    }
}
