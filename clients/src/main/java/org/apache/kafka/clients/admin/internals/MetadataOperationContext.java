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

package org.apache.kafka.clients.admin.internals;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;

/**
 * Context class to encapsulate parameters of a call to fetch and use cluster metadata.
 * Some of the parameters are provided at construction and are immutable whereas others are provided
 * as "Call" are completed and values are available.
 *
 * @param <T> The type of return value of the KafkaFuture
 * @param <O> The type of configuration option.
 */
public final class MetadataOperationContext<T, O extends AbstractOptions<O>> {
    final private Collection<String> topics;
    final private O options;
    final private long deadline;
    final private Map<TopicPartition, KafkaFutureImpl<T>> futures;
    private Optional<MetadataResponse> response;

    public MetadataOperationContext(Collection<String> topics,
                                    O options,
                                    long deadline,
                                    Map<TopicPartition, KafkaFutureImpl<T>> futures) {
        this.topics = topics;
        this.options = options;
        this.deadline = deadline;
        this.futures = futures;
        this.response = Optional.empty();
    }

    public void setResponse(Optional<MetadataResponse> response) {
        this.response = response;
    }

    public Optional<MetadataResponse> response() {
        return response;
    }

    public O options() {
        return options;
    }

    public long deadline() {
        return deadline;
    }

    public Map<TopicPartition, KafkaFutureImpl<T>> futures() {
        return futures;
    }

    public Collection<String> topics() {
        return topics;
    }

    public static void handleMetadataErrors(MetadataResponse response) {
        for (TopicMetadata tm : response.topicMetadata()) {
            if (shouldRefreshMetadata(tm.error())) throw tm.error().exception();
            for (PartitionMetadata pm : tm.partitionMetadata()) {
                if (shouldRefreshMetadata(pm.error)) {
                    throw pm.error.exception();
                }
            }
        }
    }

    public static boolean shouldRefreshMetadata(Errors error) {
        return error.exception() instanceof InvalidMetadataException;
    }
}