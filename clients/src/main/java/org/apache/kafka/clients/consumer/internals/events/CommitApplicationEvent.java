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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.DefaultAsyncCoordinator;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Timer;

import java.util.Map;
import java.util.Optional;

public class CommitApplicationEvent extends ApplicationEvent {
    public RequestFuture<Void> commitFuture;
    public final Map<TopicPartition, OffsetAndMetadata> offsets;
    public final Optional<OffsetCommitCallback> callback;
    public final Timer timer;
    public final boolean isAsync;

    public CommitApplicationEvent(
            final Map<TopicPartition, OffsetAndMetadata> offsets,
            final OffsetCommitCallback callback,
            final Timer timer) {
        super(Type.COMMIT);
        this.offsets = offsets;
        this.callback = Optional.ofNullable(callback);
        this.commitFuture = new RequestFuture<>();
        this.timer = timer;
        this.isAsync = false;
    }

    public CommitApplicationEvent(
            final Map<TopicPartition, OffsetAndMetadata> offsets,
            final OffsetCommitCallback callback) {
        super(Type.COMMIT);
        this.offsets = offsets;
        this.callback = Optional.ofNullable(callback);
        this.commitFuture = new RequestFuture<>();
        this.timer = null;
        this.isAsync = true;
    }

    @Override
    public boolean process() {
        return false;
    }
    public boolean process(DefaultAsyncCoordinator coordinator) {
        coordinator.commitOffsets(offsets, commitFuture);
        return false;
    }
}
