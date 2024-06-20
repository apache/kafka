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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
public class RetryWithToleranceOperator<T> implements AutoCloseable {

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
    private final ErrorHandlingMetrics errorHandlingMetrics;
    private final CountDownLatch stopRequestedLatch;
    private volatile boolean stopping;   // indicates whether the operator has been asked to stop retrying
    private List<ErrorReporter<T>> reporters;

    public RetryWithToleranceOperator(long errorRetryTimeout, long errorMaxDelayInMillis,
                                      ToleranceType toleranceType, Time time, ErrorHandlingMetrics errorHandlingMetrics) {
        this(errorRetryTimeout, errorMaxDelayInMillis, toleranceType, time, errorHandlingMetrics, new CountDownLatch(1));
    }

    RetryWithToleranceOperator(long errorRetryTimeout, long errorMaxDelayInMillis,
                               ToleranceType toleranceType, Time time, ErrorHandlingMetrics errorHandlingMetrics,
                               CountDownLatch stopRequestedLatch) {
        this.errorRetryTimeout = errorRetryTimeout;
        this.errorMaxDelayInMillis = errorMaxDelayInMillis;
        this.errorToleranceType = toleranceType;
        this.time = time;
        this.errorHandlingMetrics = errorHandlingMetrics;
        this.stopRequestedLatch = stopRequestedLatch;
        this.stopping = false;
        this.reporters = Collections.emptyList();
    }

    /**
     * Inform this class that some external operation has already failed. This is used when the control flow does not
     * allow for the operation to be started and stopped within the scope of a single {@link Operation}, and the
     * {@link #execute(ProcessingContext, Operation, Stage, Class)} method cannot be used.
     *
     * @param context The {@link ProcessingContext} used to hold state about this operation
     * @param stage The logical stage within the overall pipeline of the operation that has failed
     * @param executingClass The class containing the operation implementation that failed
     * @param error The error which caused the operation to fail
     * @return A future which resolves when this failure has been persisted by all {@link ErrorReporter} instances
     * @throws ConnectException if the operation is not tolerated, and the overall pipeline should stop
     */
    public Future<Void> executeFailed(ProcessingContext<T> context, Stage stage, Class<?> executingClass, Throwable error) {
        markAsFailed();
        context.currentContext(stage, executingClass);
        context.error(error);
        errorHandlingMetrics.recordFailure();
        Future<Void> errantRecordFuture = report(context);
        if (!withinToleranceLimits()) {
            errorHandlingMetrics.recordError();
            throw new ConnectException("Tolerance exceeded in error handler", error);
        }
        return errantRecordFuture;
    }

    /**
     * Report an error to all configured {@link ErrorReporter} instances.
     * @param context The context containing details of the error to report
     * @return A future which resolves when this failure has been persisted by all {@link ErrorReporter} instances
     */
    // Visible for testing
    synchronized Future<Void> report(ProcessingContext<T> context) {
        if (reporters.size() == 1) {
            return new WorkerErrantRecordReporter.ErrantRecordFuture(Collections.singletonList(reporters.iterator().next().report(context)));
        }
        List<Future<RecordMetadata>> futures = reporters.stream()
                .map(r -> r.report(context))
                .filter(f -> !f.isDone())
                .collect(Collectors.toList());
        if (futures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return new WorkerErrantRecordReporter.ErrantRecordFuture(futures);
    }

    /**
     * Attempt to execute an operation. Handles retriable and tolerated exceptions thrown by the operation. This is
     * used for small blocking operations which can be represented as an {@link Operation}. For operations which do
     * not fit this interface, see {@link #executeFailed(ProcessingContext, Stage, Class, Throwable)}
     *
     * <p>If any error is already present in the context, return null without modifying the context.
     * <p>Retries are allowed if the operator is still running, and this operation is within the error retry timeout.
     * <p>Tolerable exceptions are different for each stage, and encoded in {@link #TOLERABLE_EXCEPTIONS}
     * <p>This method mutates the passed-in {@link ProcessingContext} with the number of attempts made to execute the
     * operation, and the last error encountered if no attempt was successful.
     *
     * @param context The {@link ProcessingContext} used to hold state about this operation
     * @param operation the recoverable operation
     * @param stage The logical stage within the overall pipeline of the operation that has failed
     * @param executingClass The class containing the operation implementation that failed
     * @param <V> return type of the result of the operation.
     * @return result of the operation, or null if a prior exception occurred, or the operation only threw retriable or tolerable exceptions
     * @throws ConnectException wrapper if any non-tolerated exception was thrown by the operation
     */
    public <V> V execute(ProcessingContext<T> context, Operation<V> operation, Stage stage, Class<?> executingClass) {
        context.currentContext(stage, executingClass);
        if (context.failed()) {
            log.debug("ProcessingContext is already in failed state. Ignoring requested operation.");
            return null;
        }
        context.currentContext(stage, executingClass);
        try {
            Class<? extends Exception> ex = TOLERABLE_EXCEPTIONS.getOrDefault(context.stage(), RetriableException.class);
            return execAndHandleError(context, operation, ex);
        } finally {
            if (context.failed()) {
                errorHandlingMetrics.recordError();
                report(context);
            }
        }
    }

    /**
     * Attempt to execute an operation. Handles retriable exceptions raised by the operation.
     * <p>Retries are allowed if the operator is still running, and this operation is within the error retry timeout.
     * <p>This method mutates the passed-in {@link ProcessingContext} with the number of attempts made to execute the
     * operation, and the last error encountered if no attempt was successful.
     *
     * @param context The {@link ProcessingContext} used to hold state about this operation
     * @param operation the operation to be executed.
     * @param <V> the return type of the result of the operation.
     * @return the result of the operation if it succeeded, or null if the operation only threw retriable exceptions
     * @throws Exception rethrow if any non-retriable exception was thrown by the operation
     */
    protected <V> V execAndRetry(ProcessingContext<T> context, Operation<V> operation) throws Exception {
        int attempt = 0;
        long startTime = time.milliseconds();
        long deadline = (errorRetryTimeout >= 0) ? startTime + errorRetryTimeout : Long.MAX_VALUE;
        do {
            try {
                attempt++;
                return operation.call();
            } catch (RetriableException e) {
                log.trace("Caught a retriable exception while executing {} operation with {}", context.stage(), context.executingClass());
                errorHandlingMetrics.recordFailure();
                if (time.milliseconds() < deadline) {
                    backoff(attempt, deadline);
                    errorHandlingMetrics.recordRetry();
                } else {
                    log.trace("Can't retry. start={}, attempt={}, deadline={}", startTime, attempt, deadline);
                    context.error(e);
                    return null;
                }
                if (stopping) {
                    log.trace("Shutdown has been scheduled. Marking operation as failed.");
                    context.error(e);
                    return null;
                }
            } finally {
                context.attempt(attempt);
            }
        } while (true);
    }

    /**
     * Attempt to execute an operation. Handles retriable and tolerated exceptions thrown by the operation.
     * <p>This method mutates the passed-in {@link ProcessingContext} with the number of attempts made to execute the
     * operation, and the last error encountered if no attempt was successful.
     *
     * @param operation the operation to be executed.
     * @param tolerated the class of exceptions which can be tolerated if errors.tolerance=all
     * @param <V> The return type of the result of the operation.
     * @return the result of the operation, or null if the operation only threw retriable or tolerable exceptions
     * @throws ConnectException wrapper if any non-tolerated exception was thrown by the operation
     */
    // Visible for testing
    protected <V> V execAndHandleError(ProcessingContext<T> context, Operation<V> operation, Class<? extends Exception> tolerated) {
        try {
            V result = execAndRetry(context, operation);
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
    synchronized void markAsFailed() {
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

    /**
     * Do an exponential backoff bounded by {@link #RETRIES_DELAY_MIN_MS} and {@link #errorMaxDelayInMillis}
     * which can be exited prematurely if {@link #triggerStop()} is called or if the thread is interrupted.
     * Visible for testing.
     * @param attempt the number indicating which backoff attempt it is (beginning with 1)
     * @param deadline the time in milliseconds until when retries can be attempted
     */
    void backoff(int attempt, long deadline) {
        int numRetry = attempt - 1;
        long delay = RETRIES_DELAY_MIN_MS << numRetry;
        if (delay > errorMaxDelayInMillis) {
            delay = ThreadLocalRandom.current().nextLong(errorMaxDelayInMillis);
        }
        long currentTime = time.milliseconds();
        if (delay + currentTime > deadline) {
            delay = Math.max(0, deadline - currentTime);
        }
        log.debug("Sleeping for up to {} millis", delay);
        try {
            stopRequestedLatch.await(delay, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return;
        }
    }

    @Override
    public String toString() {
        return "RetryWithToleranceOperator{" +
                "errorRetryTimeout=" + errorRetryTimeout +
                ", errorMaxDelayInMillis=" + errorMaxDelayInMillis +
                ", errorToleranceType=" + errorToleranceType +
                ", totalFailures=" + totalFailures +
                ", time=" + time +
                '}';
    }

    /**
     * Set the error reporters for this connector.
     *
     * @param reporters the error reporters (should not be null).
     */
    public synchronized void reporters(List<ErrorReporter<T>> reporters) {
        this.reporters = Objects.requireNonNull(reporters, "reporters");
    }

    /**
     * This will stop any further retries for operations.
     * This will also mark any ongoing operations that are currently backing off for retry as failed.
     * This can be called from a separate thread to break out of retry/backoff loops in
     * {@link #execAndRetry(ProcessingContext, Operation)}
     */
    public void triggerStop() {
        stopping = true;
        stopRequestedLatch.countDown();
    }

    @Override
    public synchronized void close() {
        ConnectException e = null;
        for (ErrorReporter<T> reporter : reporters) {
            try {
                reporter.close();
            } catch (Throwable t) {
                e = e != null ? e : new ConnectException("Failed to close all reporters");
                e.addSuppressed(t);
            }
        }
        reporters = Collections.emptyList();
        if (e != null) {
            throw e;
        }
    }
}
