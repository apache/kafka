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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The result of the {@link Admin#listTopics()} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListTopicsResult {
    final KafkaFuture<Map<String, TopicListing>> future;

    ListTopicsResult(KafkaFuture<Map<String, TopicListing>> future) {
        this.future = future;
    }

    /**
     * Return a future which yields a map of topic names to TopicListing objects.
     */
    public KafkaFuture<Map<String, TopicListing>> namesToListings() {
        return future;
    }

    /**
     * Return a future which yields a collection of TopicListing objects.
     */
    public KafkaFuture<Collection<TopicListing>> listings() {
        return future.thenApply(namesToDescriptions -> namesToDescriptions.values());
    }

    /**
     * Return a future which yields a collection of topic names.
     */
    public KafkaFuture<Set<String>> names() {
        return future.thenApply(namesToListings -> namesToListings.keySet());
    }
}
