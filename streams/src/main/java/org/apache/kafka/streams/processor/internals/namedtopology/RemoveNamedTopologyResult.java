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
package org.apache.kafka.streams.processor.internals.namedtopology;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Objects;

public class RemoveNamedTopologyResult {
    private final KafkaFutureImpl<Void> removeTopologyFuture;
    private final KafkaFutureImpl<Void> deleteOffsetsResult;

    public RemoveNamedTopologyResult(final KafkaFutureImpl<Void> removeTopologyFuture, final KafkaFutureImpl<Void> deleteOffsetsResult) {
        this.removeTopologyFuture = removeTopologyFuture;
        this.deleteOffsetsResult = deleteOffsetsResult;
    }

    public RemoveNamedTopologyResult(final KafkaFutureImpl<Void> removeTopologyFuture) {
        // Go ahead and complete this future right away if the user didn't opt to reset offsets
        this(removeTopologyFuture, new KafkaFutureImpl<>());
        deleteOffsetsResult.complete(null);
        Objects.requireNonNull(removeTopologyFuture);
    }

    public KafkaFuture<Void> removeTopologyFuture() {
        return removeTopologyFuture;
    }

    public KafkaFuture<Void> deleteOffsetsResult() {
        return deleteOffsetsResult;
    }

    /**
     * @return a {@link KafkaFuture} that completes successfully when all threads on this client have removed the
     * corresponding {@link NamedTopology} and all source topic offsets have been deleted (if applicable). At this
     * point no more of its tasks will be processed by the current client, but there may still be other clients which
     * do. It is only guaranteed that this {@link NamedTopology} has fully stopped processing when all clients have
     * successfully completed their corresponding {@link KafkaFuture}.
     */
    public final KafkaFuture<Void> all() {
        return KafkaFuture.allOf(removeTopologyFuture, deleteOffsetsResult);
    }
}