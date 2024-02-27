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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName;
import org.apache.kafka.common.KafkaException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Event that signifies that the application thread has executed the {@link ConsumerRebalanceListener} callback. If
 * the callback execution threw an error, it is included in the event should any event listener want to know.
 */
public class ConsumerRebalanceListenerCallbackCompletedEvent extends ApplicationEvent {

    private final ConsumerRebalanceListenerMethodName methodName;
    private final CompletableFuture<Void> future;
    private final Optional<KafkaException> error;

    public ConsumerRebalanceListenerCallbackCompletedEvent(final ConsumerRebalanceListenerMethodName methodName,
                                                           final CompletableFuture<Void> future,
                                                           final Optional<KafkaException> error) {
        super(Type.CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED);
        this.methodName = Objects.requireNonNull(methodName);
        this.future = Objects.requireNonNull(future);
        this.error = Objects.requireNonNull(error);
    }

    public ConsumerRebalanceListenerMethodName methodName() {
        return methodName;
    }

    public CompletableFuture<Void> future() {
        return future;
    }

    public Optional<KafkaException> error() {
        return error;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
                ", methodName=" + methodName +
                ", future=" + future +
                ", error=" + error;
    }
}
