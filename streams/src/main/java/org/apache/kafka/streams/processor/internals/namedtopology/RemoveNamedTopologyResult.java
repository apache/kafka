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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

@Deprecated
public class RemoveNamedTopologyResult {
    private final KafkaFutureImpl<Void> removeTopologyFuture;
    private final KafkaFutureImpl<Void> resetOffsetsFuture;

    public RemoveNamedTopologyResult(final KafkaFutureImpl<Void> removeTopologyFuture) {

        Objects.requireNonNull(removeTopologyFuture);
        this.removeTopologyFuture = removeTopologyFuture;
        this.resetOffsetsFuture = null;
    }

    public RemoveNamedTopologyResult(final KafkaFutureImpl<Void> removeTopologyFuture,
                                     final String removedTopology,
                                     final Runnable resetOffsets) {
        Objects.requireNonNull(removeTopologyFuture);
        this.removeTopologyFuture = removeTopologyFuture;
        resetOffsetsFuture = new ResetOffsetsFuture(removedTopology, removeTopologyFuture, resetOffsets);
    }

    public KafkaFuture<Void> removeTopologyFuture() {
        return removeTopologyFuture;
    }

    public KafkaFuture<Void> resetOffsetsFuture() {
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
        if (resetOffsetsFuture == null) {
            return removeTopologyFuture;
        } else {
            return resetOffsetsFuture;
        }
    }

    private static class ResetOffsetsFuture extends KafkaFutureImpl<Void> {
        private final Logger log;

        final Runnable resetOffsets;
        final KafkaFutureImpl<Void> removeTopologyFuture;

        public ResetOffsetsFuture(final String removedTopology,
                                  final KafkaFutureImpl<Void> removeTopologyFuture,
                                  final Runnable resetOffsets) {
            final LogContext logContext = new LogContext(String.format("topology [%s]", removedTopology));
            this.log = logContext.logger(this.getClass());

            this.resetOffsets = resetOffsets;
            this.removeTopologyFuture = removeTopologyFuture;
        }

        @Override
        public Void get() throws ExecutionException {
            final AtomicReference<Throwable> firstError = new AtomicReference<>(null);
            try {
                removeTopologyFuture.get();
            } catch (final InterruptedException | ExecutionException e) {
                final Throwable error = e.getCause() != null ? e.getCause() : e;
                log.error("Removing named topology failed. Offset reset will still be attempted.", error);
                firstError.compareAndSet(e, null);
            }
            try {
                resetOffsets.run();
            } catch (final Throwable e) {
                log.error("Failed to reset offsets, you should do so manually if you want to add new topologies"
                              + "in the future that consume from the same input topics");
                firstError.compareAndSet(e, null);
            }

            if (firstError.get() != null) {
                throw new ExecutionException(firstError.get());
            } else {
                return null;
            }
        }


    }
}
