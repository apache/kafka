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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * The result of the {@link KafkaAdminClient#describeTopics(Collection)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeTopicsResult {
    private final Map<Uuid, KafkaFuture<TopicDescription>> topicIdFutures;
    private final Map<String, KafkaFuture<TopicDescription>> nameFutures;

    // VisibleForTesting
    protected DescribeTopicsResult(Map<Uuid, KafkaFuture<TopicDescription>> topicIdFutures, Map<String, KafkaFuture<TopicDescription>> nameFutures) {
        if (topicIdFutures != null && nameFutures != null)
            throw new IllegalArgumentException("topicIdFutures and nameFutures cannot both be specified.");
        if (topicIdFutures == null && nameFutures == null)
            throw new IllegalArgumentException("topicIdFutures and nameFutures cannot both be null.");
        this.topicIdFutures = topicIdFutures;
        this.nameFutures = nameFutures;
    }

    static DescribeTopicsResult ofTopicIds(Map<Uuid, KafkaFuture<TopicDescription>> topicIdFutures) {
        return new DescribeTopicsResult(topicIdFutures, null);
    }

    static DescribeTopicsResult ofTopicNames(Map<String, KafkaFuture<TopicDescription>> nameFutures) {
        return new DescribeTopicsResult(null, nameFutures);
    }

    /**
     * Use when {@link Admin#describeTopics(TopicCollection, DescribeTopicsOptions)} used a TopicIdCollection
     *
     * @return a map from topic IDs to futures which can be used to check the status of
     *         individual topics if the request used topic IDs, otherwise return null.
     */
    public Map<Uuid, KafkaFuture<TopicDescription>> topicIdValues() {
        return topicIdFutures;
    }

    /**
     * Use when {@link Admin#describeTopics(TopicCollection, DescribeTopicsOptions)} used a TopicNameCollection
     *
     * @return a map from topic names to futures which can be used to check the status of
     *         individual topics if the request used topic names, otherwise return null.
     */
    public Map<String, KafkaFuture<TopicDescription>> topicNameValues() {
        return nameFutures;
    }

    /**
     * @return A future map from topic names to descriptions which can be used to check
     *         the status of individual description if the describe topic request used
     *         topic names, otherwise return null, this request succeeds only if all the
     *         topic descriptions succeed
     */
    public KafkaFuture<Map<String, TopicDescription>> allTopicNames() {
        return all(nameFutures);
    }

    /**
     * @return A future map from topic ids to descriptions which can be used to check the
     *         status of individual description if the describe topic request used topic
     *         ids, otherwise return null, this request succeeds only if all the topic
     *         descriptions succeed
     */
    public KafkaFuture<Map<Uuid, TopicDescription>> allTopicIds() {
        return all(topicIdFutures);
    }

    /**
     * Return a future which succeeds only if all the topic descriptions succeed.
     */
    private static <T> KafkaFuture<Map<T, TopicDescription>> all(Map<T, KafkaFuture<TopicDescription>> futures) {
        if (futures == null) return null;
        KafkaFuture<Void> future = KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
        return future.
            thenApply(v -> {
                Map<T, TopicDescription> descriptions = new HashMap<>(futures.size());
                for (Map.Entry<T, KafkaFuture<TopicDescription>> entry : futures.entrySet()) {
                    try {
                        descriptions.put(entry.getKey(), entry.getValue().get());
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, because allOf ensured that all the futures
                        // completed successfully.
                        throw new RuntimeException(e);
                    }
                }
                return descriptions;
            });
    }
}
