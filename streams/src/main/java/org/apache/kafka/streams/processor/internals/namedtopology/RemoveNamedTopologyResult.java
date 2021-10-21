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

import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;

public class RemoveNamedTopologyResult {
    private final KafkaFuture<Void> removeTopologyFuture;
    private final DeleteConsumerGroupOffsetsResult deleteOffsetsResult;

    public RemoveNamedTopologyResult(final KafkaFuture<Void> removeTopologyFuture, final DeleteConsumerGroupOffsetsResult deleteOffsetsResult) {
        this.removeTopologyFuture = removeTopologyFuture;
        this.deleteOffsetsResult = deleteOffsetsResult;
    }

    public RemoveNamedTopologyResult(final KafkaFuture<Void> removeTopologyFuture) {
        this(removeTopologyFuture, null);
    }

    public KafkaFuture<Void> removeTopologyFuture() {
        return removeTopologyFuture;
    }

    public DeleteConsumerGroupOffsetsResult deleteOffsetsResult() {
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
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        if (deleteOffsetsResult != null) {
            deleteOffsetsResult.all().whenComplete((ignore, throwable) -> {
                if (throwable != null) {
                    result.completeExceptionally(throwable);
                }
            });
        }

        removeTopologyFuture.whenComplete((ignore, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                result.complete(null);
            }
        });
        return result;
    }
}
