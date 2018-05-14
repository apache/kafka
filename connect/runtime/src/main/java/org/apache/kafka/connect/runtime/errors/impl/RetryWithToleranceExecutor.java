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
package org.apache.kafka.connect.runtime.errors.impl;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.OperationExecutor;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.ToleranceExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

public class RetryWithToleranceExecutor extends OperationExecutor {

    private static final Logger log = LoggerFactory.getLogger(RetryWithToleranceExecutor.class);

    public static final String RETRIES_LIMIT = "retries.limit";
    public static final String RETRIES_LIMIT_DOC = "The maximum number of retries before failing an operation";
    public static final long RETRIES_LIMIT_DEFAULT = 0;

    public static final String RETRIES_DELAY_MAX_MS = "retries.delay.max.ms";
    public static final String RETRIES_DELAY_MAX_MS_DOC = "The maximum duration between two consecutive retries (in milliseconds).";
    public static final long RETRIES_DELAY_MAX_MS_DEFAULT = 60000;

    public static final long RETRIES_DELAY_MIN_MS = 1000;

    public static final String TOLERANCE_LIMIT = "tolerance.limit";
    public static final String TOLERANCE_LIMIT_DOC = "Fail the task if we exceed specified number of errors overall.";
    public static final long TOLERANCE_LIMIT_DEFAULT = 0;

    public static final String TOLERANCE_RATE_LIMIT = "tolerance.rate.limit";
    public static final String TOLERANCE_RATE_LIMIT_DOC = "Fail the task if we exceed specified number of errors in the observed duration.";
    public static final long TOLERANCE_RATE_LIMIT_DEFAULT = 0;

    public static final String TOLERANCE_RATE_DURATION = "tolerance.rate.duration";
    public static final String TOLERANCE_RATE_DURATION_DOC = "The duration of the window for which we will monitor errors.";
    public static final String TOLERANCE_RATE_DURATION_DEFAULT = "minute";

    private final Time time;
    private RetryWithToleranceExecutorConfig config;

    private long totalFailures = 0;
    private long totalFailuresInDuration = 0;
    private long durationWindow = 0;
    private long durationStart = 0;

    public RetryWithToleranceExecutor() {
        this(new SystemTime());
    }

    public RetryWithToleranceExecutor(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new RetryWithToleranceExecutorConfig(configs);
        durationWindow = TimeUnit.MILLISECONDS.convert(1, config.toleranceRateDuration());
        durationStart = time.milliseconds() % durationWindow;
    }

    static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(RETRIES_LIMIT, ConfigDef.Type.LONG, RETRIES_LIMIT_DEFAULT, ConfigDef.Importance.HIGH, RETRIES_LIMIT_DOC)
                .define(RETRIES_DELAY_MAX_MS, ConfigDef.Type.LONG, RETRIES_DELAY_MAX_MS_DEFAULT, atLeast(1), ConfigDef.Importance.MEDIUM, RETRIES_DELAY_MAX_MS_DOC)
                .define(TOLERANCE_LIMIT, ConfigDef.Type.LONG, TOLERANCE_LIMIT_DEFAULT, ConfigDef.Importance.HIGH, TOLERANCE_LIMIT_DOC)
                .define(TOLERANCE_RATE_LIMIT, ConfigDef.Type.LONG, TOLERANCE_RATE_LIMIT_DEFAULT, ConfigDef.Importance.MEDIUM, TOLERANCE_RATE_LIMIT_DOC)
                .define(TOLERANCE_RATE_DURATION, ConfigDef.Type.STRING, TOLERANCE_RATE_DURATION_DEFAULT, in("minute", "hour", "day"), ConfigDef.Importance.MEDIUM, TOLERANCE_RATE_DURATION_DOC);
    }

    @Override
    public <V> V execute(Operation<V> operation, V value, ProcessingContext context) {
        try {
            switch (context.current().type()) {
                case TRANSFORMATION:
                case HEADER_CONVERTER:
                case KEY_CONVERTER:
                case VALUE_CONVERTER:
                    return execAndHandleError(operation, value, context, Exception.class);
                case KAFKA_PRODUCE:
                case KAFKA_CONSUME:
                    return execAndHandleError(operation, value, context, org.apache.kafka.common.errors.RetriableException.class);
                default:
                    return execAndHandleError(operation, value, context, org.apache.kafka.connect.errors.RetriableException.class);
            }
        } finally {
            if (context.exception() != null) {
                context.report();
            }
        }
    }

    private <V> V execAndHandleError(Operation<V> operation, V value, ProcessingContext context, Class<? extends Exception> handled) {
        Response<V> response = new Response<>();
        boolean retry;
        do {
            apply(response, operation, handled);
            context.incrementAttempt();
            switch (response.status) {
                case SUCCESS:
                    return response.result;
                case RETRY:
                    context.setException(response.ex);
                    retry = checkRetry(context);
                    log.trace("Operation failed. For attempt={}, limit={}, retry={}", context.attempt(), config.retriesLimit(), retry);
                    break;
                case UNHANDLED_EXCEPTION:
                    context.setException(response.ex);
                    throw new ConnectException(response.ex);
                default:
                    throw new ConnectException("Undefined state: " + response.status);
            }
            if (retry) {
                backoff(context);
                if (Thread.currentThread().isInterrupted()) {
                    // thread was interrupted during sleep. kill the task.
                    throw new ConnectException("Thread was interrupted");
                }
            }
        } while (retry);

        // mark this record as failed.
        totalFailures++;
        context.setTimeOfError(time.milliseconds());

        if (!withinToleranceLimits(context)) {
            throw new ToleranceExceededException("Tolerance Limit Exceeded", response.ex);
        }

        log.trace("Operation failed but within tolerance limits. Returning default value={}", value);
        return value;
    }

    // Visible for testing
    protected boolean withinToleranceLimits(ProcessingContext context) {
        final long timeOfError = context.timeOfError();
        long newDurationStart = timeOfError - timeOfError % durationWindow;
        if (newDurationStart > durationStart) {
            durationStart = newDurationStart;
            totalFailuresInDuration = 0;
        }
        totalFailuresInDuration++;

        log.info("Marking the record as failed, totalFailures={}, totalFailuresInDuration={}", totalFailures, totalFailuresInDuration);

        if (totalFailures > config.toleranceLimit() || totalFailuresInDuration > config.toleranceRateLimit()) {
            return false;
        }

        return true;
    }

    // Visible for tests
    protected boolean checkRetry(ProcessingContext context) {
        long limit = config.retriesLimit();
        if (limit == -1) {
            return true;
        } else if (limit == 0) {
            return false;
        } else if (limit > 0) {
            // number of retries is one less than the number of attempts.
            return (context.attempt() - 1) < config.retriesLimit();
        } else {
            log.error("Unexpected value for retry limit={}. Will disable retry.", limit);
            return false;
        }
    }

    protected void backoff(ProcessingContext context) {
        int numRetry = context.attempt() - 1;
        long delay = RETRIES_DELAY_MIN_MS << numRetry;
        if (delay > config.retriesDelayMax()) {
            delay = ThreadLocalRandom.current().nextLong(config.retriesDelayMax());
        }

        log.debug("Sleeping for {} millis", delay);
        time.sleep(delay);
    }

    private <V> void apply(Response<V> response, Operation<V> operation, Class<? extends Exception> handled) {
        try {
            response.result = operation.apply();
            response.ex = null;
            response.status = ResponseStatus.SUCCESS;
        } catch (Exception e) {
            response.ex = e;
            response.result = null;
            if (handled.isAssignableFrom(e.getClass())) {
                response.status = ResponseStatus.RETRY;
            } else {
                response.status = ResponseStatus.UNHANDLED_EXCEPTION;
            }
        }
    }

    static class Response<V> {
        ResponseStatus status;
        Exception ex;
        V result;
    }

    enum ResponseStatus {
        SUCCESS,
        RETRY,
        UNHANDLED_EXCEPTION
    }

    static class RetryWithToleranceExecutorConfig extends AbstractConfig {

        public RetryWithToleranceExecutorConfig(Map<?, ?> originals) {
            super(getConfigDef(), originals);
        }

        public long retriesLimit() {
            return getLong(RETRIES_LIMIT);
        }

        public long retriesDelayMax() {
            return getLong(RETRIES_DELAY_MAX_MS);
        }

        public long toleranceLimit() {
            return getLong(TOLERANCE_LIMIT);
        }

        public long toleranceRateLimit() {
            return getLong(TOLERANCE_RATE_LIMIT);
        }

        public TimeUnit toleranceRateDuration() {
            final String duration = getString(TOLERANCE_RATE_DURATION);
            switch (duration.toLowerCase(Locale.ROOT)) {
                case "minute":
                    return TimeUnit.MINUTES;
                case "hour":
                    return TimeUnit.HOURS;
                case "day":
                    return TimeUnit.DAYS;
                default:
                    throw new ConfigException("Could not recognize value " + getString(TOLERANCE_LIMIT) + " for config " + TOLERANCE_RATE_DURATION);
            }
        }
    }
}
