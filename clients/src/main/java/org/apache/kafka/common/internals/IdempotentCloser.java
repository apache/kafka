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
package org.apache.kafka.common.internals;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * {@code IdempotentCloser} encapsulates some basic logic to ensure that a given resource is only closed once.
 * The underlying mechanism for ensuring that the close only happens once <em>and</em> is thread safe
 * is via the {@link AtomicBoolean#compareAndSet(boolean, boolean)}. Users can provide callbacks (via optional
 * {@link Runnable}s) for either the <em>initial</em> close and/or any <em>subsequent</em> closes.
 *
 * <p/>
 *
 * Here's an example:
 *
 * <pre>
 *
 * public class MyDataFile implements Closeable {
 *
 *     private final IdempotentCloser closer = new IdempotentCloser();
 *
 *     private final File file;
 *
 *     . . .
 *
 *     public boolean write() {
 *         closer.assertOpen(() -> String.format("Data file %s already closed!", file));
 *         writeToFile();
 *     }
 *
 *     public boolean isClosed() {
 *         return closer.isClosed();
 *     }
 *
 *     &#064;Override
 *     public void close() {
 *         Runnable onInitialClose = () -> {
 *             cleanUpFile(file);
 *             log.debug("Data file {} closed", file);
 *         };
 *         Runnable onSubsequentClose = () -> {
 *             log.warn("Data file {} already closed!", file);
 *         };
 *         closer.close(onInitialClose, onSubsequentClose);
 *     }
 * }
 * </pre>
 */
public class IdempotentCloser implements AutoCloseable {

    private final AtomicBoolean isClosed;

    /**
     * Creates an {@code IdempotentCloser} that is not yet closed.
     */
    public IdempotentCloser() {
        this(false);
    }

    /**
     * Creates an {@code IdempotentCloser} with the given initial state.
     *
     * @param isClosed Initial value for underlying state
     */
    public IdempotentCloser(boolean isClosed) {
        this.isClosed = new AtomicBoolean(isClosed);
    }

    /**
     * This method serves as an assert that the {@link IdempotentCloser} is still open. If it is open, this method
     * simply returns. If it is closed, a new {@link IllegalStateException} will be thrown using the supplied message.
     *
     * @param message {@link Supplier} that supplies the message for the exception
     */
    public void assertOpen(Supplier<String> message) {
        if (isClosed.get())
            throw new IllegalStateException(message.get());
    }

    /**
     * This method serves as an assert that the {@link IdempotentCloser} is still open. If it is open, this method
     * simply returns. If it is closed, a new {@link IllegalStateException} will be thrown using the given message.
     *
     * @param message Message to use for the exception
     */
    public void assertOpen(String message) {
        if (isClosed.get())
            throw new IllegalStateException(message);
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * Closes the resource in a thread-safe manner.
     *
     * <p/>
     *
     * After the execution has completed, calls to {@link #isClosed()} will return {@code false} and calls to
     * {@link #assertOpen(String)} and {@link #assertOpen(Supplier)}
     * will throw an {@link IllegalStateException}.
     */
    @Override
    public void close() {
        close(null, null);
    }

    /**
     * Closes the resource in a thread-safe manner.
     *
     * <p/>
     *
     * After the execution has completed, calls to {@link #isClosed()} will return {@code false} and calls to
     * {@link #assertOpen(String)} and {@link #assertOpen(Supplier)}
     * will throw an {@link IllegalStateException}.
     *
     * @param onInitialClose Optional {@link Runnable} to execute when the resource is closed. Note that the
     *                       object will still be considered closed even if an exception is thrown during the course
     *                       of its execution; can be {@code null}
     */
    public void close(final Runnable onInitialClose) {
        close(onInitialClose, null);
    }

    /**
     * Closes the resource in a thread-safe manner.
     *
     * <p/>
     *
     * After the execution has completed, calls to {@link #isClosed()} will return {@code false} and calls to
     * {@link #assertOpen(String)} and {@link #assertOpen(Supplier)}
     * will throw an {@link IllegalStateException}.
     *
     * @param onInitialClose    Optional {@link Runnable} to execute when the resource is closed. Note that the
     *                          object will still be considered closed even if an exception is thrown during the course
     *                          of its execution; can be {@code null}
     * @param onSubsequentClose Optional {@link Runnable} to execute if this resource was previously closed. Note that
     *                          no state will be affected if an exception is thrown during its execution; can be
     *                          {@code null}
     */
    public void close(final Runnable onInitialClose, final Runnable onSubsequentClose) {
        if (isClosed.compareAndSet(false, true)) {
            if (onInitialClose != null)
                onInitialClose.run();
        } else {
            if (onSubsequentClose != null)
                onSubsequentClose.run();
        }
    }

    @Override
    public String toString() {
        return "IdempotentCloser{" +
                "isClosed=" + isClosed +
                '}';
    }
}