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
package org.apache.kafka.server.util;

import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;


public class FutureUtils {
    /**
     * Wait for a future until a specific time in the future, with copious logging.
     *
     * @param log           The slf4j object to use to log success and failure.
     * @param action        The action we are waiting for.
     * @param future        The future we are waiting for.
     * @param deadline      The deadline in the future we are waiting for.
     * @param time          The clock object.
     *
     * @return              The result of the future.
     * @param <T>           The type of the future.
     *
     * @throws java.util.concurrent.TimeoutException If the future times out.
     * @throws Throwable If the future fails. Note: we unwrap ExecutionException here.
     */
    public static <T> T waitWithLogging(
        Logger log,
        String prefix,
        String action,
        CompletableFuture<T> future,
        Deadline deadline,
        Time time
    ) throws Throwable {
        log.info("{}Waiting for {}", prefix, action);
        try {
            T result = time.waitForFuture(future, deadline.nanoseconds());
            log.info("{}Finished waiting for {}", prefix, action);
            return result;
        } catch (TimeoutException t)  {
            log.error("{}Timed out while waiting for {}", prefix, action, t);
            TimeoutException timeout = new TimeoutException("Timed out while waiting for " + action);
            timeout.setStackTrace(t.getStackTrace());
            throw timeout;
        } catch (Throwable t)  {
            if (t instanceof ExecutionException) {
                ExecutionException executionException = (ExecutionException) t;
                t = executionException.getCause();
            }
            log.error("{}Received a fatal error while waiting for {}", prefix, action, t);
            throw new RuntimeException("Received a fatal error while waiting for " + action, t);
        }
    }

    /**
     * Complete a given destination future when a source future is completed.
     *
     * @param sourceFuture          The future to trigger off of.
     * @param destinationFuture     The future to complete when the source future is completed.
     * @param <T>                   The destination future type.
     */
    public static <T> void chainFuture(
        CompletableFuture<? extends T> sourceFuture,
        CompletableFuture<T> destinationFuture
    ) {
        sourceFuture.whenComplete((BiConsumer<T, Throwable>) (val, throwable) -> {
            if (throwable != null) {
                destinationFuture.completeExceptionally(throwable);
            } else {
                destinationFuture.complete(val);
            }
        });
    }

    /**
     * Returns a new CompletableFuture that is already completed exceptionally with the given exception.
     *
     * @param ex    The exception.
     * @return      The exceptionally completed CompletableFuture.
     */
    public static <T> CompletableFuture<T> failedFuture(Throwable ex) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }
}
