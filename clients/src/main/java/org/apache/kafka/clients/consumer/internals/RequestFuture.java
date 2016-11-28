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

import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.Errors;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Result of an asynchronous request from {@link ConsumerNetworkClient}. Use {@link ConsumerNetworkClient#poll(long)}
 * (and variants) to finish a request future. Use {@link #isDone()} to check if the future is complete, and
 * {@link #succeeded()} to check if the request completed successfully. Typical usage might look like this:
 *
 * <pre>
 *     RequestFuture<ClientResponse> future = client.send(api, request);
 *     client.poll(future);
 *
 *     if (future.succeeded()) {
 *         ClientResponse response = future.value();
 *         // Handle response
 *     } else {
 *         throw future.exception();
 *     }
 * </pre>
 *
 * @param <T> Return type of the result (Can be Void if there is no response)
 */
public class RequestFuture<T> implements ConsumerNetworkClient.PollCondition {

    private static final Object INCOMPLETE_SENTINEL = new Object();
    private final AtomicReference<Object> result = new AtomicReference<>(INCOMPLETE_SENTINEL);
    private final ConcurrentLinkedQueue<RequestFutureListener<T>> listeners = new ConcurrentLinkedQueue<>();

    /**
     * Check whether the response is ready to be handled
     * @return true if the response is ready, false otherwise
     */
    public boolean isDone() {
        return result.get() != INCOMPLETE_SENTINEL;
    }

    /**
     * Get the value corresponding to this request (only available if the request succeeded)
     * @return the value set in {@link #complete(Object)}
     * @throws IllegalStateException if the future is not complete or failed
     */
    @SuppressWarnings("unchecked")
    public T value() {
        if (!succeeded())
            throw new IllegalStateException("Attempt to retrieve value from future which hasn't successfully completed");
        return (T) result.get();
    }

    /**
     * Check if the request succeeded;
     * @return true if the request completed and was successful
     */
    public boolean succeeded() {
        return isDone() && !failed();
    }

    /**
     * Check if the request failed.
     * @return true if the request completed with a failure
     */
    public boolean failed() {
        return result.get() instanceof RuntimeException;
    }

    /**
     * Check if the request is retriable (convenience method for checking if
     * the exception is an instance of {@link RetriableException}.
     * @return true if it is retriable, false otherwise
     * @throws IllegalStateException if the future is not complete or completed successfully
     */
    public boolean isRetriable() {
        return exception() instanceof RetriableException;
    }

    /**
     * Get the exception from a failed result (only available if the request failed)
     * @return the exception set in {@link #raise(RuntimeException)}
     * @throws IllegalStateException if the future is not complete or completed successfully
     */
    public RuntimeException exception() {
        if (!failed())
            throw new IllegalStateException("Attempt to retrieve exception from future which hasn't failed");
        return (RuntimeException) result.get();
    }

    /**
     * Complete the request successfully. After this call, {@link #succeeded()} will return true
     * and the value can be obtained through {@link #value()}.
     * @param value corresponding value (or null if there is none)
     * @throws IllegalStateException if the future has already been completed
     * @throws IllegalArgumentException if the argument is an instance of {@link RuntimeException}
     */
    public void complete(T value) {
        if (value instanceof RuntimeException)
            throw new IllegalArgumentException("The argument to complete can not be an instance of RuntimeException");

        if (!result.compareAndSet(INCOMPLETE_SENTINEL, value))
            throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");
        fireSuccess();
    }

    /**
     * Raise an exception. The request will be marked as failed, and the caller can either
     * handle the exception or throw it.
     * @param e corresponding exception to be passed to caller
     * @throws IllegalStateException if the future has already been completed
     */
    public void raise(RuntimeException e) {
        if (e == null)
            throw new IllegalArgumentException("The exception passed to raise must not be null");

        if (!result.compareAndSet(INCOMPLETE_SENTINEL, e))
            throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");

        fireFailure();
    }

    /**
     * Raise an error. The request will be marked as failed.
     * @param error corresponding error to be passed to caller
     */
    public void raise(Errors error) {
        raise(error.exception());
    }

    private void fireSuccess() {
        T value = value();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null)
                break;
            listener.onSuccess(value);
        }
    }

    private void fireFailure() {
        RuntimeException exception = exception();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null)
                break;
            listener.onFailure(exception);
        }
    }

    /**
     * Add a listener which will be notified when the future completes
     * @param listener non-null listener to add
     */
    public void addListener(RequestFutureListener<T> listener) {
        this.listeners.add(listener);
        if (failed())
            fireFailure();
        else if (succeeded())
            fireSuccess();
    }

    /**
     * Convert from a request future of one type to another type
     * @param adapter The adapter which does the conversion
     * @param <S> The type of the future adapted to
     * @return The new future
     */
    public <S> RequestFuture<S> compose(final RequestFutureAdapter<T, S> adapter) {
        final RequestFuture<S> adapted = new RequestFuture<>();
        addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                adapter.onSuccess(value, adapted);
            }

            @Override
            public void onFailure(RuntimeException e) {
                adapter.onFailure(e, adapted);
            }
        });
        return adapted;
    }

    public void chain(final RequestFuture<T> future) {
        addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                future.complete(value);
            }

            @Override
            public void onFailure(RuntimeException e) {
                future.raise(e);
            }
        });
    }

    public static <T> RequestFuture<T> failure(RuntimeException e) {
        RequestFuture<T> future = new RequestFuture<>();
        future.raise(e);
        return future;
    }

    public static RequestFuture<Void> voidSuccess() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);
        return future;
    }

    public static <T> RequestFuture<T> coordinatorNotAvailable() {
        return failure(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.exception());
    }

    public static <T> RequestFuture<T> leaderNotAvailable() {
        return failure(Errors.LEADER_NOT_AVAILABLE.exception());
    }

    public static <T> RequestFuture<T> noBrokersAvailable() {
        return failure(new NoAvailableBrokersException());
    }

    public static <T> RequestFuture<T> staleMetadata() {
        return failure(new StaleMetadataException());
    }

    @Override
    public boolean shouldBlock() {
        return !isDone();
    }
}
