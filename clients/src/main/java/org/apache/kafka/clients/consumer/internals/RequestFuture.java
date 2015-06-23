/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

/**
 * Result of an asynchronous request through {@link org.apache.kafka.clients.KafkaClient}. To get the
 * result of the request, you must use poll using {@link org.apache.kafka.clients.KafkaClient#poll(long, long)}
 * until {@link #isDone()} returns true. Typical usage might look like this:
 *
 * <pre>
 *     RequestFuture future = sendRequest();
 *     while (!future.isDone()) {
 *         client.poll(timeout, now);
 *     }
 *
 *     switch (future.outcome()) {
 *     case SUCCESS:
 *         // handle request success
 *         break;
 *     case NEED_RETRY:
 *         // retry after taking possible retry action
 *         break;
 *     case EXCEPTION:
 *         // handle exception
  *     }
 * </pre>
 *
 * When {@link #isDone()} returns true, there are three possible outcomes (obtained through {@link #outcome()}):
 *
 * <ol>
 * <li> {@link org.apache.kafka.clients.consumer.internals.RequestFuture.Outcome#SUCCESS}: If the request was
 *    successful, then you can use {@link #value()} to obtain the result.</li>
 * <li> {@link org.apache.kafka.clients.consumer.internals.RequestFuture.Outcome#EXCEPTION}: If an unhandled exception
 *    was encountered, you can use {@link #exception()} to get it.</li>
 * <li> {@link org.apache.kafka.clients.consumer.internals.RequestFuture.Outcome#NEED_RETRY}: The request may
 *    not have been successful, but the failure may be ephemeral and the caller just needs to try the request again.
 *    In this case, use {@link #retryAction()} to determine what action should be taken (if any) before
 *    retrying.</li>
 * </ol>
 *
 * @param <T> Return type of the result (Can be Void if there is no response)
 */
public class RequestFuture<T> {
    public static final RequestFuture<Object> NEED_NEW_COORDINATOR = newRetryFuture(RetryAction.FIND_COORDINATOR);
    public static final RequestFuture<Object> NEED_POLL = newRetryFuture(RetryAction.POLL);
    public static final RequestFuture<Object> NEED_METADATA_REFRESH = newRetryFuture(RetryAction.REFRESH_METADATA);

    public enum RetryAction {
        NOOP,             // Retry immediately.
        POLL,             // Retry after calling poll (e.g. to finish a connection)
        BACKOFF,          // Retry after a delay
        FIND_COORDINATOR, // Find a new coordinator before retrying
        REFRESH_METADATA  // Refresh metadata before retrying
    }

    public enum Outcome {
        SUCCESS,
        NEED_RETRY,
        EXCEPTION
    }

    private Outcome outcome;
    private RetryAction retryAction;
    private T value;
    private RuntimeException exception;

    /**
     * Check whether the response is ready to be handled
     * @return true if the response is ready, false otherwise
     */
    public boolean isDone() {
        return outcome != null;
    }

    /**
     * Get the value corresponding to this request (if it has one, as indicated by {@link #outcome()}).
     * @return the value if it exists or null
     */
    public T value() {
        return value;
    }

    /**
     * Check if the request succeeded;
     * @return true if a value is available, false otherwise
     */
    public boolean succeeded() {
        return outcome == Outcome.SUCCESS;
    }

    /**
     * Check if the request completed failed.
     * @return true if the request failed (whether or not it can be retried)
     */
    public boolean failed() {
        return outcome != Outcome.SUCCESS;
    }

    /**
     * Return the error from this response (assuming {@link #succeeded()} has returned false. If the
     * response is not ready or if there is no retryAction, null is returned.
     * @return the error if it exists or null
     */
    public RetryAction retryAction() {
        return retryAction;
    }

    /**
     * Get the exception from a failed result. You should check that there is an exception
     * with {@link #hasException()} before using this method.
     * @return The exception if it exists or null
     */
    public RuntimeException exception() {
        return exception;
    }

    /**
     * Check whether there was an exception.
     * @return true if this request failed with an exception
     */
    public boolean hasException() {
        return outcome == Outcome.EXCEPTION;
    }

    /**
     * Check the outcome of the future if it is ready.
     * @return the outcome or null if the future is not finished
     */
    public Outcome outcome() {
        return outcome;
    }

    /**
     * The request failed, but should be retried using the provided retry action.
     * @param retryAction The action that should be taken by the caller before retrying the request
     */
    public void retry(RetryAction retryAction) {
        this.outcome = Outcome.NEED_RETRY;
        this.retryAction = retryAction;
    }

    public void retryNow() {
        retry(RetryAction.NOOP);
    }

    public void retryAfterBackoff() {
        retry(RetryAction.BACKOFF);
    }

    public void retryWithNewCoordinator() {
        retry(RetryAction.FIND_COORDINATOR);
    }

    public void retryAfterMetadataRefresh() {
        retry(RetryAction.REFRESH_METADATA);
    }

    /**
     * Complete the request successfully. After this call, {@link #succeeded()} will return true
     * and the value can be obtained through {@link #value()}.
     * @param value corresponding value (or null if there is none)
     */
    public void complete(T value) {
        this.outcome = Outcome.SUCCESS;
        this.value = value;
    }

    /**
     * Raise an exception. The request will be marked as failed, and the caller can either
     * handle the exception or throw it.
     * @param e The exception that
     */
    public void raise(RuntimeException e) {
        this.outcome = Outcome.EXCEPTION;
        this.exception = e;
    }

    private static <T> RequestFuture<T> newRetryFuture(RetryAction retryAction) {
        RequestFuture<T> result = new RequestFuture<T>();
        result.retry(retryAction);
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> RequestFuture<T> pollNeeded() {
        return (RequestFuture<T>) NEED_POLL;
    }

    @SuppressWarnings("unchecked")
    public static <T> RequestFuture<T> metadataRefreshNeeded() {
        return (RequestFuture<T>) NEED_METADATA_REFRESH;
    }

    @SuppressWarnings("unchecked")
    public static <T> RequestFuture<T> newCoordinatorNeeded() {
        return (RequestFuture<T>) NEED_NEW_COORDINATOR;
    }

}
