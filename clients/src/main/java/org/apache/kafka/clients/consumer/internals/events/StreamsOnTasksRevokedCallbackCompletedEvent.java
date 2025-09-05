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

import org.apache.kafka.common.KafkaException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class StreamsOnTasksRevokedCallbackCompletedEvent extends ApplicationEvent {

    private final CompletableFuture<Void> future;
    private final Optional<KafkaException> error;

    public StreamsOnTasksRevokedCallbackCompletedEvent(final CompletableFuture<Void> future,
                                                       final Optional<KafkaException> error) {
        super(Type.STREAMS_ON_TASKS_REVOKED_CALLBACK_COMPLETED);
        this.future = Objects.requireNonNull(future);
        this.error = Objects.requireNonNull(error);
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
            ", future=" + future +
            ", error=" + error;
    }
}
