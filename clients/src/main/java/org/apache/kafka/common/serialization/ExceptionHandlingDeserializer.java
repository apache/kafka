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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.header.Headers;

import java.util.Map;

/**
 * When a {@link Deserializer} deserializes a message, it may throws an exception. E.g. a decryption deserializer fails to decrypt a corrupted message. When this happens, the {@link Deserializer} exits and stop consuming messages. This is often undesirable as it means the calling code is unable to take action, even reconnecting to the topic might simple cause the exception to happen again.
 * <p>
 * {@link ExceptionHandlingDeserializer} is a decorator than allows you to wrap up another deserializer and return a result that contains either the deserialized result, or any exception throw by the deserializer.
 * <p>
 * You may configure using properties
 * <p>
 * <code>
 *     exception.handling.deserializer.delegate=org.apache.kafka.common.deserialization.ListDeserializer
 * </codd>
 *
 * @param <T>
 */
public class ExceptionHandlingDeserializer<T> implements Deserializer<ExceptionHandlingDeserializer.Result<T>> {

    public static class Result<T> {
        private final T result;
        private final Exception exception;

        Result(T result, Exception exception) {
            this.result = result;
            this.exception = exception;
        }

        public T getResult() {
            return result;
        }

        public Exception getException() {
            return exception;
        }
    }

    private Deserializer<T> delegate;

    public ExceptionHandlingDeserializer() {
    }

    public ExceptionHandlingDeserializer(Deserializer<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            delegate = (Deserializer<T>) Class.forName(String.valueOf(configs.get("exception.handling.deserializer.delegate"))).asSubclass(Deserializer.class).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        delegate.configure(configs, isKey);
    }

    @Override
    public Result<T> deserialize(String topic, byte[] data) {
        try {
            return new Result<>(delegate.deserialize(topic, data), null);
        } catch (Exception e) {
            return new Result<>(null, e);
        }
    }

    @Override
    public Result<T> deserialize(String topic, Headers headers, byte[] data) {
        try {
            return new Result<>(delegate.deserialize(topic, headers, data), null);
        } catch (Exception e) {
            return new Result<>(null, e);
        }
    }
}
