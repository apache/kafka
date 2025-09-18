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
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.ThreadSafeAutoCommitState.AUTO_COMMIT_DISABLED;

/**
 * This class stores shared state needed by both the application thread ({@link AsyncKafkaConsumer}) and the
 * network thread ({@link ConsumerNetworkThread}) for the {@link AsyncKafkaConsumer}.
 */
public class ThreadSafeAsyncConsumerState extends ThreadSafeConsumerState {

    private final ThreadSafeAutoCommitState autoCommitState;

    public ThreadSafeAsyncConsumerState(ThreadSafeAutoCommitState autoCommitState) {
        this.autoCommitState = requireNonNull(autoCommitState);
    }

    public static ThreadSafeAsyncConsumerState fromConfig(LogContext logContext, ConsumerConfig config, Time time) {
        return new ThreadSafeAsyncConsumerState(ThreadSafeAutoCommitState.fromConfig(logContext, config, time));
    }

    static ThreadSafeAsyncConsumerState withAutoCommitEnabled(final LogContext logContext, final Time time) {
        int defaultAutoCommitIntervalMs = 5000;     // This is as defined in ConsumerConfig
        return new ThreadSafeAsyncConsumerState(
            new ThreadSafeAutoCommitState.AutoCommitEnabled(
                logContext,
                time,
                defaultAutoCommitIntervalMs
            )
        );
    }

    static ThreadSafeAsyncConsumerState withAutoCommitDisabled() {
        return new ThreadSafeAsyncConsumerState(AUTO_COMMIT_DISABLED);
    }

    public ThreadSafeAutoCommitState autoCommitState() {
        return autoCommitState;
    }

    /**
     * This method is used by {@code AsyncKafkaConsumer#poll()} to determine if it can skip waiting for the
     * {@link PollEvent}. Sending the {@link PollEvent} is in the critical path, and if the application thread
     * can determine that it doesn't need to wait for it to complete before continuing, that is a big performance
     * savings.
     *
     * <p/>
     *
     * This method performs similar checks to the start of {@code ApplicationEventProcessor#process(PollEvent)}:
     *
     * <ol>
     *     <li>
     *         Checks if there is already a reconciliation in process in
     *         {@link AbstractMembershipManager#maybeReconcile(boolean)}
     *     </li>
     *     <li>
     *         Checks if the auto-commit's interval has expired and needs to perform a commit offsets operation
     *         in {@link CommitRequestManager#updateTimerAndMaybeCommit(long)}
     *     </li>
     * </ol>
     *
     * If either of the above tests are satisfied, this method will return {@code false} to let the application thread
     * know that it needs to block for the {@link PollEvent} to complete. Otherwise, this method will return
     * {@code true}, which signals to the application thread that it can enqueue a {@link PollEvent} but it should
     * not wait for it to complete.
     *
     * @return true if all checks pass, false if either of the latter two checks fail
     */
    public boolean canSkipWaitingOnPoll(long currentTimeMs) {
        if (reconciliationState.isInProgress())
            return false;

        autoCommitState.updateTimer(currentTimeMs);
        return !autoCommitState.shouldAutoCommit();
    }
}
