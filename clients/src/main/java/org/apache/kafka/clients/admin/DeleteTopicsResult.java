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
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;

/**
 * The result of the {@link Admin#deleteTopics(Collection)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DeleteTopicsResult {
    private final Map<Uuid, KafkaFuture<Void>> topicIdFutures;
    private final Map<String, KafkaFuture<Void>> nameFutures;

    protected DeleteTopicsResult(Map<Uuid, KafkaFuture<Void>> topicIdFutures, Map<String, KafkaFuture<Void>> nameFutures) {
        if (topicIdFutures != null && nameFutures != null)
            throw new IllegalArgumentException("topicIdFutures and nameFutures cannot both be specified.");
        if (topicIdFutures == null && nameFutures == null)
            throw new IllegalArgumentException("topicIdFutures and nameFutures cannot both be null.");
        this.topicIdFutures = topicIdFutures;
        this.nameFutures = nameFutures;
    }

    static DeleteTopicsResult ofTopicIds(Map<Uuid, KafkaFuture<Void>> topicIdFutures) {
        return new DeleteTopicsResult(topicIdFutures, null);
    }

    static DeleteTopicsResult ofTopicNames(Map<String, KafkaFuture<Void>> nameFutures) {
        return new DeleteTopicsResult(null, nameFutures);
    }

    /**
     * Use when {@link Admin#deleteTopics(TopicCollection, DeleteTopicsOptions)} used a TopicIdCollection
     * @return a map from topic IDs to futures which can be used to check the status of
     * individual deletions if the deleteTopics request used topic IDs. Otherwise return null.
     */
    public Map<Uuid, KafkaFuture<Void>> topicIdValues() {
        return topicIdFutures;
    }

    /**
     * Use when {@link Admin#deleteTopics(TopicCollection, DeleteTopicsOptions)} used a TopicNameCollection
     * @return a map from topic names to futures which can be used to check the status of
     * individual deletions if the deleteTopics request used topic names. Otherwise return null.
     */
    public Map<String, KafkaFuture<Void>> topicNameValues() {
        return nameFutures;
    }

    /**
     * @return a map from topic names to futures which can be used to check the status of
     * individual deletions if the deleteTopics request used topic names. Otherwise return null.
     * @deprecated Since 3.0 use {@link #topicNameValues} instead
     */
    @Deprecated
    public Map<String, KafkaFuture<Void>> values() {
        return nameFutures;
    }

    /**
     * @return a future which succeeds only if all the topic deletions succeed.
     */
    public KafkaFuture<Void> all() {
        return (topicIdFutures == null) ? KafkaFuture.allOf(nameFutures.values().toArray(new KafkaFuture[0])) :
            KafkaFuture.allOf(topicIdFutures.values().toArray(new KafkaFuture[0]));
    }
}
