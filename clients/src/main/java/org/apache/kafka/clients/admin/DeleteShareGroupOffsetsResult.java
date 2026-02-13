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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Map;
import java.util.Set;

/**
 * The result of the {@link Admin#deleteShareGroupOffsets(String, Set, DeleteShareGroupOffsetsOptions)} call.
 */
public class DeleteShareGroupOffsetsResult {

    private final KafkaFuture<Map<String, ApiException>> future;
    private final Set<String> topics;

    DeleteShareGroupOffsetsResult(KafkaFuture<Map<String, ApiException>> future, Set<String> topics) {
        this.future = future;
        this.topics = topics;
    }

    /**
     * Return a future which succeeds only if all the deletions succeed.
     * If not, the first topic error shall be returned.
     */
    public KafkaFuture<Void> all() {
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        this.future.whenComplete((topicResults, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                for (String topic : topics) {
                    if (maybeCompleteExceptionally(topicResults, topic, result)) {
                        return;
                    }
                }
                result.complete(null);
            }
        });
        return result;
    }

    /**
     * Return a future which can be used to check the result for a given topic.
     */
    public KafkaFuture<Void> topicResult(final String topic) {
        if (!topics.contains(topic)) {
            throw new IllegalArgumentException("Topic " + topic + " was not included in the original request");
        }
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        this.future.whenComplete((topicResults, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else if (!maybeCompleteExceptionally(topicResults, topic, result)) {
                result.complete(null);
            }
        });
        return result;
    }

    private boolean maybeCompleteExceptionally(Map<String, ApiException> topicLevelErrors,
                                               String topic,
                                               KafkaFutureImpl<Void> result) {
        Throwable exception;
        if (!topicLevelErrors.containsKey(topic)) {
            exception = new IllegalArgumentException("Offset deletion result for topic \"" + topic + "\" was not included in the response");
        } else {
            exception = topicLevelErrors.get(topic);
        }

        if (exception != null) {
            result.completeExceptionally(exception);
            return true;
        } else {
            return false;
        }
    }
}
