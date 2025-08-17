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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_AUTO_COMMIT_INTERVAL_MS;

/**
 * Encapsulates the state of auto-committing and manages the auto-commit timer.
 */
public interface AutoCommitState {

    /**
     * @return {@code true} if auto-commit is enabled as defined in the configuration
     * {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG}.
     */
    boolean isAutoCommitEnabled();

    boolean shouldAutoCommit();

    boolean isExpired();

    /**
     * Reset the auto-commit timer to the auto-commit interval, so that the next auto-commit is
     * sent out on the interval starting from now. If auto-commit is disabled this will
     * perform no action.
     */
    void resetTimer();

    /**
     * Reset the auto-commit timer to the provided time (backoff), so that the next auto-commit is
     * sent out then. If auto-commit is disabled this will perform no action.
     */
    void resetTimer(long retryBackoffMs);

    long remainingMs(final long currentTimeMs);

    void updateTimer(final long currentTimeMs);

    void setInflightCommitStatus(final boolean inflightCommitStatus);

    static AutoCommitState enabled(final LogContext logContext,
                                   final Time time,
                                   final long autoCommitInterval) {
        return new AutoCommitStateEnabled(logContext, time, autoCommitInterval);
    }

    static AutoCommitState enabled(final LogContext logContext, final Time time) {
        return enabled(logContext, time, DEFAULT_AUTO_COMMIT_INTERVAL_MS);
    }

    static AutoCommitState disabled() {
        return new AutoCommitStateDisabled();
    }

    static AutoCommitState newInstance(final LogContext logContext,
                                       final ConsumerConfig config,
                                       final Time time) {
        if (config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            final long interval = Integer.toUnsignedLong(config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            return enabled(logContext, time, interval);
        } else {
            return disabled();
        }
    }

    class AutoCommitStateEnabled implements AutoCommitState {

        private final Logger log;
        private final Timer timer;
        private final long autoCommitInterval;
        private final AtomicBoolean hasInflightCommit;

        private AutoCommitStateEnabled(final LogContext logContext,
                                       final Time time,
                                       final long autoCommitInterval) {
            this.log = logContext.logger(AutoCommitState.class);
            this.timer = time.timer(autoCommitInterval);
            this.autoCommitInterval = autoCommitInterval;
            this.hasInflightCommit = new AtomicBoolean();
        }

        @Override
        public boolean isAutoCommitEnabled() {
            return true;
        }

        @Override
        public synchronized boolean shouldAutoCommit() {
            if (!timer.isExpired()) {
                return false;
            }

            if (hasInflightCommit.get()) {
                log.trace("Skipping auto-commit on the interval because a previous one is still in-flight.");
                return false;
            }

            return true;
        }

        @Override
        public synchronized boolean isExpired() {
            return timer.isExpired();
        }

        @Override
        public synchronized void resetTimer() {
            timer.reset(autoCommitInterval);
        }

        @Override
        public synchronized void resetTimer(long retryBackoffMs) {
            timer.reset(retryBackoffMs);
        }

        @Override
        public synchronized long remainingMs(final long currentTimeMs) {
            timer.update(currentTimeMs);
            return timer.remainingMs();
        }

        @Override
        public synchronized void updateTimer(final long currentTimeMs) {
            timer.update(currentTimeMs);
        }

        @Override
        public synchronized void setInflightCommitStatus(final boolean inflightCommitStatus) {
            hasInflightCommit.set(inflightCommitStatus);
        }
    }

    class AutoCommitStateDisabled implements AutoCommitState {

        private AutoCommitStateDisabled() {
        }

        @Override
        public boolean isAutoCommitEnabled() {
            return false;
        }

        @Override
        public boolean shouldAutoCommit() {
            return false;
        }

        @Override
        public boolean isExpired() {
            return false;
        }

        @Override
        public void resetTimer() {
            // No op
        }

        @Override
        public void resetTimer(long retryBackoffMs) {
            // No op
        }

        @Override
        public long remainingMs(final long currentTimeMs) {
            return Long.MAX_VALUE;
        }

        @Override
        public void updateTimer(final long currentTimeMs) {
            // No op
        }

        @Override
        public void setInflightCommitStatus(final boolean inflightCommitStatus) {
            // No op
        }
    }
}
