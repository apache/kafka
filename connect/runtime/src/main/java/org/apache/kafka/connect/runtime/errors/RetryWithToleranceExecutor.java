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

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * <p>
 * Attempt to recover a failed operation with retries and tolerance limits.
 * </p>
 *
 * <p>
 * A retry is attempted if the operation throws a {@link RetriableException}. Retries are accompanied by exponential backoffs, starting with
 * {@link #RETRIES_DELAY_MIN_MS}, up to what is specified with {@link RetryWithToleranceExecutorConfig#retryDelayMax()}.
 * Including the first attempt and future retries, the total time taken to evaluate the operation should be within
 * {@link RetryWithToleranceExecutorConfig#retryDelayMax()} millis.
 * </p>
 *
 * <p>
 * This executor will tolerate failures, as specified by {@link RetryWithToleranceExecutorConfig#toleranceLimit()}.
 * For transformations and converters, all exceptions are tolerated. For others operations, only {@link RetriableException} are tolerated.
 * </p>
 */
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

    public static final RetryWithToleranceExecutor NOOP_EXECUTOR = new RetryWithToleranceExecutor();
    static {
        NOOP_EXECUTOR.configure(Collections.emptyMap());
    }

    private static final Map<Stage, Class<? extends Exception>> TOLERABLE_EXCEPTIONS = new HashMap<>();
    static {
        TOLERABLE_EXCEPTIONS.put(Stage.TRANSFORMATION, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.HEADER_CONVERTER, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.KEY_CONVERTER, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.VALUE_CONVERTER, Exception.class);
    }

    private long totalFailures = 0;
    private final Time time;
    private RetryWithToleranceExecutorConfig config;
    private ErrorHandlingMetrics errorHandlingMetrics;

    public RetryWithToleranceExecutor() {
        this(new SystemTime());
    }

    // Visible for testing
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
            Class<? extends Exception> ex = TOLERABLE_EXCEPTIONS.getOrDefault(context.stage(), RetriableException.class);
            return execAndHandleError(operation, context, ex);
        } finally {
            if (!context.result().success()) {
                errorHandlingMetrics.recordError();
                context.report();
            }
        }
    }

    /**
     * attempt to execute an operation. Retry if a {@link RetriableException} is raised. Re-throw everything else.
     * @param operation the operation to be executed.
     * @param context the processing context.
     * @param <V> the return type of the result of the operation.
     * @return the result of the operation.
     * @throws Exception rethrow if a non-retriable Exception is thrown by the operation
     */
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
                errorHandlingMetrics.recordFailure();
                if (checkRetry(startTime)) {
                    backoff(attempt, deadline);
                    if (Thread.currentThread().isInterrupted()) {
                        log.trace("Thread was interrupted. Marking operation as failed.");
                        return new Result<>(e);
                    }
                    errorHandlingMetrics.recordRetry();
                } else {
                    log.trace("Can't retry. start={}, attempt={}, deadline={}", startTime, attempt, deadline);
                    return new Result<>(e);
                }
            } finally {
                context.attempt(attempt);
            }
        } while (true);
    }

    /**
     * Execute a given operation multiple times (if needed), and tolerate certain exceptions.
     *
     * @param operation the operation to be executed.
     * @param context the processing context.
     * @param tolerated the class of exceptions which can be tolerated.
     * @param <V> the return type of the result of the operation.
     * @return the result of the operation
     */
    // Visible for testing
    protected <V> Result<V> execAndHandleError(Operation<V> operation, ProcessingContext context, Class<? extends Exception> tolerated) {
        try {
            Result<V> result = execAndRetry(operation, context);
            if (!result.success()) {
                markAsFailed();
                errorHandlingMetrics.recordSkipped();
            }
            context.result(result);
            return result;
        } catch (Exception e) {
            errorHandlingMetrics.recordFailure();
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

            errorHandlingMetrics.recordSkipped();
            return exResult;
        }
    }

    // Visible for testing
    void markAsFailed() {
        errorHandlingMetrics.recordErrorTimestamp();
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

    public void setMetrics(ErrorHandlingMetrics errorHandlingMetrics) {
        this.errorHandlingMetrics = errorHandlingMetrics;
    }

    static class RetryWithToleranceExecutorConfig extends AbstractConfig {
        public RetryWithToleranceExecutorConfig(Map<?, ?> originals) {
            super(getConfigDef(), originals, true);
        }

        /**
         * @return the total time an operation can take to succeed (including the first attempt and retries).
         */
        public long retryTimeout() {
            return getLong(RETRY_TIMEOUT);
        }

        /**
         * @return the maximum delay between two subsequent retries in milliseconds.
         */
        public long retryDelayMax() {
            return getLong(RETRY_DELAY_MAX_MS);
        }

        /**
         * @return determine how many errors to tolerate.
         */
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
