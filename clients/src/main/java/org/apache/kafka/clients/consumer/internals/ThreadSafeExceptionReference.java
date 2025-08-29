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

import org.apache.kafka.common.KafkaException;

import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * {@code ThreadSafeExceptionReference} builds on top of {@link ThreadSafeReference} both to be more explicit
 * about the contents and to provide utility methods.
 */
public class ThreadSafeExceptionReference extends ThreadSafeReference<Throwable> {

    private static final Consumer<Throwable> THROW_EXCEPTION = exception -> {
        // Unwrap the ExecutionException to model what ConsumerUtils.getResult() does when handling exceptions
        // from the call to Future.get().
        if (exception instanceof CompletionException)
            exception = exception.getCause();

        throw ConsumerUtils.maybeWrapAsKafkaException(exception);
    };

    /**
     * If the underlying error is present, this will throw the error <em>and</em> clear it.
     *
     * <p/>
     *
     * Note: if the exception is wrapped in a {@link CompletionException}, it will be unwrapped. However, if
     * the underlying error is <em>not</em> a subclass of {@link KafkaException}, it will be wrapped as such
     * so that it is an unchecked exception.
     */
    public void maybeClearAndThrowException() {
        getClearAndRun(THROW_EXCEPTION);
    }

    /**
     * If the underlying error is present, this will throw the error.
     *
     * <p/>
     *
     * Note: if the exception is wrapped in a {@link CompletionException}, it will be unwrapped. However, if
     * the underlying error is <em>not</em> a subclass of {@link KafkaException}, it will be wrapped as such
     * so that it is an unchecked exception.
     */
    public void maybeThrowException() {
        ifPresent(THROW_EXCEPTION);
    }
}
