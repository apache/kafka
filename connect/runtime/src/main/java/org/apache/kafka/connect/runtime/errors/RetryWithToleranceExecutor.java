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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

public class RetryWithToleranceExecutor implements OperationExecutor {

    private static final Logger log = LoggerFactory.getLogger(RetryWithToleranceExecutor.class);

    public static final String RETRY_TIMEOUT = "retry.timeout";
    public static final String RETRY_TIMEOUT_DOC = "The maximum number of retries before failing an operation";
    public static final long RETRY_TIMEOUT_DEFAULT = 0;

    public static final String RETRY_DELAY_MAX_MS = "retry.delay.max.ms";
    public static final String RETRY_DELAY_MAX_MS_DOC = "The maximum duration between two consecutive retries (in milliseconds).";
    public static final long RETRY_DELAY_MAX_MS_DEFAULT = 60000;

    public static final long RETRIES_DELAY_MIN_MS = 300;

    public static final String TOLERANCE_LIMIT = "allowed.max";
    public static final String TOLERANCE_LIMIT_DOC = "Fail the task if we exceed specified number of errors overall.";
    public static final String TOLERANCE_LIMIT_DEFAULT = "none";

    private long totalFailures = 0;
    private final Time time;
    private RetryWithToleranceExecutorConfig config;

    public RetryWithToleranceExecutor() {
        this(new SystemTime());
    }

    public RetryWithToleranceExecutor(Time time) {
        this.time = time;
    }

    static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(RETRY_TIMEOUT, ConfigDef.Type.LONG, RETRY_TIMEOUT_DEFAULT, ConfigDef.Importance.HIGH, RETRY_TIMEOUT_DOC)
                .define(RETRY_DELAY_MAX_MS, ConfigDef.Type.LONG, RETRY_DELAY_MAX_MS_DEFAULT, atLeast(1), ConfigDef.Importance.MEDIUM, RETRY_DELAY_MAX_MS_DOC)
                .define(TOLERANCE_LIMIT, ConfigDef.Type.STRING, TOLERANCE_LIMIT_DEFAULT, in("none", "all"), ConfigDef.Importance.HIGH, TOLERANCE_LIMIT_DOC);
    }

    @Override
    public <V> Result<V> execute(Operation<V> operation, ProcessingContext context) {
        try {
            switch (context.stage()) {
                case TRANSFORMATION:
                case HEADER_CONVERTER:
                case KEY_CONVERTER:
                case VALUE_CONVERTER:
                    return execAndHandleError(operation, context, Exception.class);
                default:
                    return execAndHandleError(operation, context, org.apache.kafka.connect.errors.RetriableException.class);
            }
        } finally {
            if (!context.result().success()) {
                context.report();
            }
        }
    }

    protected <V> Result<V> execAndRetry(Operation<V> operation, ProcessingContext context) throws Exception {
        int attempt = 0;
        long startTime = time.milliseconds();
        long deadline = startTime + config.retryTimeout();
        do {
            try {
                attempt++;
                V v = operation.apply();
                Result<V> result = new Result<>(v);
                context.result(result);
                return result;
            } catch (RetriableException e) {
                log.trace("Caught a retriable exception while executing {} operation with {}", context.stage(), context.executingClass());
                if (checkRetry(startTime)) {
                    backoff(attempt, deadline);
                    if (Thread.currentThread().isInterrupted()) {
                        log.trace("Thread was interrupted. Marking operation as failed.");
                        return new Result<>(e);
                    }
                } else {
                    return new Result<>(e);
                }
            } finally {
                context.attempt(attempt);
            }
        } while (true);
    }

    // Visible for testing
    protected <V> Result<V> execAndHandleError(Operation<V> operation, ProcessingContext context, Class<? extends Exception> tolerated) {
        try {
            Result<V> result = execAndRetry(operation, context);
            if (!result.success()) {
                markAsFailed();
            }
            context.result(result);
            return result;
        } catch (Exception e) {
            markAsFailed();

            Result<V> exResult = new Result<>(e);
            context.result(exResult);

            if (!tolerated.isAssignableFrom(e.getClass())) {
                context.result(new Result<>(e));
                throw new ConnectException("Unhandled exception in error handler", e);
            }

            if (!withinToleranceLimits()) {
                throw new ConnectException("Tolerance exceeded in error handler", e);
            }

            return exResult;
        }
    }

    // Visible for testing
    void markAsFailed() {
        totalFailures++;
    }

    // Visible for testing
    boolean withinToleranceLimits() {
        switch (config.toleranceLimit()) {
            case NONE:
                if (totalFailures > 0) return false;
            case ALL:
                return true;
            default:
                throw new ConfigException("Unknown tolerance type: {}", config.toleranceLimit());
        }
    }

    // Visible for testing
    boolean checkRetry(long startTime) {
        return (time.milliseconds() - startTime) < config.retryTimeout();
    }

    // Visible for testing
    void backoff(int attempt, long deadline) {
        int numRetry = attempt - 1;
        long delay = RETRIES_DELAY_MIN_MS << numRetry;
        if (delay > config.retryDelayMax()) {
            delay = ThreadLocalRandom.current().nextLong(config.retryDelayMax());
        }
        if (delay + time.milliseconds() > deadline) {
            delay = deadline - time.milliseconds();
        }
        log.debug("Sleeping for {} millis", delay);
        time.sleep(delay);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new RetryWithToleranceExecutorConfig(configs);
    }

    static class RetryWithToleranceExecutorConfig extends AbstractConfig {
        public RetryWithToleranceExecutorConfig(Map<?, ?> originals) {
            super(getConfigDef(), originals, true);
        }

        public long retryTimeout() {
            return getLong(RETRY_TIMEOUT);
        }

        public long retryDelayMax() {
            return getLong(RETRY_DELAY_MAX_MS);
        }

        public ToleranceType toleranceLimit() {
            return "ALL".equals(getString(TOLERANCE_LIMIT).toUpperCase(Locale.ROOT)) ? ToleranceType.ALL
                    : ToleranceType.NONE;
        }
    }

    @Override
    public String toString() {
        return "RetryWithToleranceExecutor{" +
                "config=" + config +
                '}';
    }
}
