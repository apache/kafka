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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Attempt to recover a failed operation with retries and tolerance limits.
 * <p>
 *
 * A retry is attempted if the operation throws a {@link RetriableException}. Retries are accompanied by exponential backoffs, starting with
 * {@link #RETRIES_DELAY_MIN_MS}, up to what is specified with {@link ConnectorConfig#errorMaxDelayInMillis()}.
 * Including the first attempt and future retries, the total time taken to evaluate the operation should be within
 * {@link ConnectorConfig#errorMaxDelayInMillis()} millis.
 * <p>
 *
 * This executor will tolerate failures, as specified by {@link ConnectorConfig#errorToleranceType()}.
 * For transformations and converters, all exceptions are tolerated. For others operations, only {@link RetriableException} are tolerated.
 * <p>
 *
 * There are three outcomes to executing an operation. It might succeed, in which case the result is returned to the caller.
 * If it fails, this class does one of these two things: (1) if the failure occurred due to a tolerable exception, then
 * set appropriate error reason in the {@link ProcessingContext} and return null, or (2) if the exception is not tolerated,
 * then it is wrapped into a ConnectException and rethrown to the caller.
 * <p>
 *
 * Instances of this class are thread safe.
 * <p>
 */
public class RetryWithToleranceOperator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RetryWithToleranceOperator.class);

    public static final long RETRIES_DELAY_MIN_MS = 300;

    private static final Map<Stage, Class<? extends Exception>> TOLERABLE_EXCEPTIONS = new HashMap<>();
    static {
        TOLERABLE_EXCEPTIONS.put(Stage.TRANSFORMATION, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.HEADER_CONVERTER, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.KEY_CONVERTER, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.VALUE_CONVERTER, Exception.class);
    }

    private final long errorRetryTimeout;
    private final long errorMaxDelayInMillis;
    private final ToleranceType errorToleranceType;

    private long totalFailures = 0;
    private final Time time;
    private ErrorHandlingMetrics errorHandlingMetrics;

    protected final ProcessingContext context;

    public RetryWithToleranceOperator(long errorRetryTimeout, long errorMaxDelayInMillis,
                                      ToleranceType toleranceType, Time time) {
        this(errorRetryTimeout, errorMaxDelayInMillis, toleranceType, time, new ProcessingContext());
    }

    RetryWithToleranceOperator(long errorRetryTimeout, long errorMaxDelayInMillis,
                               ToleranceType toleranceType, Time time,
                               ProcessingContext context) {
        this.errorRetryTimeout = errorRetryTimeout;
        this.errorMaxDelayInMillis = errorMaxDelayInMillis;
        this.errorToleranceType = toleranceType;
        this.time = time;
        this.context = context;
    }

    public synchronized Future<Void> executeFailed(Stage stage, Class<?> executingClass,
                                      ConsumerRecord<byte[], byte[]> consumerRecord,
                                      Throwable error) {

        markAsFailed();
        context.consumerRecord(consumerRecord);
        context.currentContext(stage, executingClass);
        context.error(error);
        errorHandlingMetrics.recordFailure();
        Future<Void> errantRecordFuture = context.report();
        if (!withinToleranceLimits()) {
            errorHandlingMetrics.recordError();
            throw new ConnectException("Tolerance exceeded in error handler", error);
        }
        return errantRecordFuture;
    }

    public synchronized Future<Void> executeFailed(Stage stage, Class<?> executingClass,
                                                   SourceRecord sourceRecord,
                                                   Throwable error) {

        markAsFailed();
        context.sourceRecord(sourceRecord);
        context.currentContext(stage, executingClass);
        context.error(error);
        errorHandlingMetrics.recordFailure();
        Future<Void> errantRecordFuture = context.report();
        if (!withinToleranceLimits()) {
            errorHandlingMetrics.recordError();
            throw new ConnectException("Tolerance exceeded in Source Worker error handler", error);
        }
        return errantRecordFuture;
    }

    /**
     * Execute the recoverable operation. If the operation is already in a failed state, then simply return
     * with the existing failure.
     *
     * @param operation the recoverable operation
     * @param <V> return type of the result of the operation.
     * @return result of the operation
     */
    public synchronized <V> V execute(Operation<V> operation, Stage stage, Class<?> executingClass) {
        context.currentContext(stage, executingClass);

        if (context.failed()) {
            log.debug("ProcessingContext is already in failed state. Ignoring requested operation.");
            return null;
        }

        try {
            Class<? extends Exception> ex = TOLERABLE_EXCEPTIONS.getOrDefault(context.stage(), RetriableException.class);
            return execAndHandleError(operation, ex);
        } finally {
            if (context.failed()) {
                errorHandlingMetrics.recordError();
                context.report();
            }
        }
    }

    /**
     * Attempt to execute an operation. Retry if a {@link RetriableException} is raised. Re-throw everything else.
     *
     * @param operation the operation to be executed.
     * @param <V> the return type of the result of the operation.
     * @return the result of the operation.
     * @throws Exception rethrow if a non-retriable Exception is thrown by the operation
     */
    protected <V> V execAndRetry(Operation<V> operation) throws Exception {
        int attempt = 0;
        long startTime = time.milliseconds();
        long deadline = startTime + errorRetryTimeout;
        do {
            try {
                attempt++;
                return operation.call();
            } catch (RetriableException e) {
                log.trace("Caught a retriable exception while executing {} operation with {}", context.stage(), context.executingClass());
                errorHandlingMetrics.recordFailure();
                if (checkRetry(startTime)) {
                    backoff(attempt, deadline);
                    if (Thread.currentThread().isInterrupted()) {
                        log.trace("Thread was interrupted. Marking operation as failed.");
                        context.error(e);
                        return null;
                    }
                    errorHandlingMetrics.recordRetry();
                } else {
                    log.trace("Can't retry. start={}, attempt={}, deadline={}", startTime, attempt, deadline);
                    context.error(e);
                    return null;
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
     * @param tolerated the class of exceptions which can be tolerated.
     * @param <V> The return type of the result of the operation.
     * @return the result of the operation
     */
    // Visible for testing
    protected <V> V execAndHandleError(Operation<V> operation, Class<? extends Exception> tolerated) {
        try {
            V result = execAndRetry(operation);
            if (context.failed()) {
                markAsFailed();
                errorHandlingMetrics.recordSkipped();
            }
            return result;
        } catch (Exception e) {
            errorHandlingMetrics.recordFailure();
            markAsFailed();
            context.error(e);

            if (!tolerated.isAssignableFrom(e.getClass())) {
                throw new ConnectException("Unhandled exception in error handler", e);
            }

            if (!withinToleranceLimits()) {
                throw new ConnectException("Tolerance exceeded in error handler", e);
            }

            errorHandlingMetrics.recordSkipped();
            return null;
        }
    }

    // Visible for testing
    void markAsFailed() {
        errorHandlingMetrics.recordErrorTimestamp();
        totalFailures++;
    }

    @SuppressWarnings("fallthrough")
    public synchronized boolean withinToleranceLimits() {
        switch (errorToleranceType) {
            case NONE:
                if (totalFailures > 0) return false;
            case ALL:
                return true;
            default:
                throw new ConfigException("Unknown tolerance type: {}", errorToleranceType);
        }
    }

    // For source connectors that want to skip kafka producer errors.
    // They cannot use withinToleranceLimits() as no failure may have actually occurred prior to the producer failing
    // to write to kafka.
    public ToleranceType getErrorToleranceType() {
        return errorToleranceType;
    }

    // Visible for testing
    boolean checkRetry(long startTime) {
        return (time.milliseconds() - startTime) < errorRetryTimeout;
    }

    // Visible for testing
    void backoff(int attempt, long deadline) {
        int numRetry = attempt - 1;
        long delay = RETRIES_DELAY_MIN_MS << numRetry;
        if (delay > errorMaxDelayInMillis) {
            delay = ThreadLocalRandom.current().nextLong(errorMaxDelayInMillis);
        }
        if (delay + time.milliseconds() > deadline) {
            delay = deadline - time.milliseconds();
        }
        log.debug("Sleeping for {} millis", delay);
        time.sleep(delay);
    }

    public synchronized void metrics(ErrorHandlingMetrics errorHandlingMetrics) {
        this.errorHandlingMetrics = errorHandlingMetrics;
    }

    @Override
    public String toString() {
        return "RetryWithToleranceOperator{" +
                "errorRetryTimeout=" + errorRetryTimeout +
                ", errorMaxDelayInMillis=" + errorMaxDelayInMillis +
                ", errorToleranceType=" + errorToleranceType +
                ", totalFailures=" + totalFailures +
                ", time=" + time +
                ", context=" + context +
                '}';
    }

    /**
     * Set the error reporters for this connector.
     *
     * @param reporters the error reporters (should not be null).
     */
    public synchronized void reporters(List<ErrorReporter> reporters) {
        this.context.reporters(reporters);
    }

    /**
     * Set the source record being processed in the connect pipeline.
     *
     * @param preTransformRecord the source record
     */
    public synchronized void sourceRecord(SourceRecord preTransformRecord) {
        this.context.sourceRecord(preTransformRecord);
    }

    /**
     * Set the record consumed from Kafka in a sink connector.
     *
     * @param consumedMessage the record
     */
    public synchronized void consumerRecord(ConsumerRecord<byte[], byte[]> consumedMessage) {
        this.context.consumerRecord(consumedMessage);
    }

    /**
     * @return true, if the last operation encountered an error; false otherwise
     */
    public synchronized boolean failed() {
        return this.context.failed();
    }

    /**
     * Returns the error encountered when processing the current stage.
     *
     * @return the error encountered when processing the current stage
     */
    public synchronized Throwable error() {
        return this.context.error();
    }

    @Override
    public synchronized void close() {
        this.context.close();
    }
}
