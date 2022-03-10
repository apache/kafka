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
package org.apache.kafka.streams.processor.internals.namedtopology;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class RemoveNamedTopologyResult {
    private final Logger log;

    private final KafkaFutureImpl<Void> removeTopologyFuture;
    private final KafkaFutureImpl<Void> resetOffsetsFuture;



    public RemoveNamedTopologyResult(final String removedTopology,
                                     final KafkaFutureImpl<Void> removeTopologyFuture) {
        final LogContext logContext = new LogContext(String.format("topology [%s]", removedTopology));
        log = logContext.logger(this.getClass());

        Objects.requireNonNull(removeTopologyFuture);
        this.removeTopologyFuture = removeTopologyFuture;

        // Go ahead and complete this future right away if the user didn't opt to reset offsets
        final KafkaFutureImpl<Void> resetOffsetsFuture = new KafkaFutureImpl<>();
        resetOffsetsFuture.complete(null);
        this.resetOffsetsFuture = resetOffsetsFuture;
    }

    public RemoveNamedTopologyResult(final String removedTopology,
                                     final KafkaFutureImpl<Void> removeTopologyFuture,
                                     final Set<TopicPartition> partitionsToReset,
                                     final String applicationId,
                                     final Admin adminClient) {
        final LogContext logContext = new LogContext(String.format("topology [%s]", removedTopology));
        log = logContext.logger(this.getClass());

        Objects.requireNonNull(removeTopologyFuture);
        this.removeTopologyFuture = removeTopologyFuture;

        resetOffsetsFuture = new ResetOffsetsFuture(applicationId, adminClient, partitionsToReset);
    }

    public KafkaFuture<Void> removeTopologyFuture() {
        return removeTopologyFuture;
    }

    public KafkaFuture<Void> deleteOffsetsResult() {
        return resetOffsetsFuture;
    }

    /**
     * @return a {@link KafkaFuture} that completes successfully when all threads on this client have removed the
     * corresponding {@link NamedTopology} and all source topic offsets have been deleted (if applicable). At this
     * point no more of its tasks will be processed by the current client, but there may still be other clients which
     * do. It is only guaranteed that this {@link NamedTopology} has fully stopped processing when all clients have
     * successfully completed their corresponding {@link KafkaFuture}.
     */
    public final KafkaFuture<Void> all() {
        return KafkaFuture.allOf(removeTopologyFuture, resetOffsetsFuture);
    }

    private class ResetOffsetsFuture extends KafkaFutureImpl<Void> {
        final String applicationId;
        final Admin adminClient;
        final Set<TopicPartition> partitionsToReset;

        public ResetOffsetsFuture(final String applicationId, final Admin adminClient, final Set<TopicPartition> partitionsToReset) {
            this.applicationId = applicationId;
            this.adminClient = adminClient;
            this.partitionsToReset = partitionsToReset;
        }

        @Override
        public Void get() {
            try {
                removeTopologyFuture.get();
            } catch (final InterruptedException | ExecutionException e) {
                final Throwable error = e.getCause() != null ? e.getCause() : e;
                log.error("Removing named topology failed Offset reset will still be attempted.", error);
            }
            try {
                resetOffsets(partitionsToReset, applicationId, adminClient);
            } catch (final Throwable e) {
                log.error("Failed to reset offsets, you should do so manually if you want to add new topologies"
                              + "in the future that consume from the same input topics");
            }

            return null;
        }

        private void resetOffsets(final Set<TopicPartition> partitionsToReset,
                                  final String applicationId,
                                  final Admin adminClient) throws Throwable {
            // The number of times to retry upon failure
            int retries = 100;
            while (true) {
                try {
                    final DeleteConsumerGroupOffsetsResult deleteOffsetsResult = adminClient.deleteConsumerGroupOffsets(applicationId, partitionsToReset);
                    deleteOffsetsResult.all().get();
                    log.info("Successfully completed resetting offsets.");
                    return;
                } catch (final InterruptedException error) {
                    error.printStackTrace();
                    log.error("Offset reset failed.", error);
                    throw error;
                } catch (final ExecutionException ex) {
                    final Throwable error = ex.getCause() != null ? ex.getCause() : ex;

                    if (error instanceof GroupSubscribedToTopicException &&
                        error.getMessage()
                            .equals("Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.")) {
                        log.debug("Offset reset failed, there may be other nodes which have not yet finished removing this topology", error);
                    } else if (error instanceof GroupIdNotFoundException) {
                        log.info("The offsets have been reset by another client or the group has been deleted, no need to retry further.");
                        return;
                    } else {
                        if (--retries > 0) {
                            log.error("Offset reset failed, retries remaining: " + retries, error);
                        } else {
                            log.error("Offset reset failed, no retries remaining.", error);
                            throw error;
                        }
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}