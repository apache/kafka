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
    private Map<String, KafkaFuture<Void>> nameFutures;
    private Map<Uuid, KafkaFuture<Void>> topicIdFutures;
    private final TopicCollection.TopicAttribute attribute;

    protected DeleteTopicsResult(TopicCollection.TopicAttribute attribute) {
        this.attribute = attribute;
    }

    private void setNameFutures(Map<String, KafkaFuture<Void>> nameFutures) {
        this.nameFutures = nameFutures;
    }

    private void setTopicIdFutures(Map<Uuid, KafkaFuture<Void>> topicIdFutures) {
        this.topicIdFutures = topicIdFutures;
    }

    protected static DeleteTopicsResult ofTopicNames(Map<String, KafkaFuture<Void>> nameFutures) {
        DeleteTopicsResult result = new DeleteTopicsResult(TopicCollection.TopicAttribute.TOPIC_NAME);
        result.setNameFutures(nameFutures);
        return result;
    }

    protected static DeleteTopicsResult ofTopicIds(Map<Uuid, KafkaFuture<Void>> topicIdFutures) {
        DeleteTopicsResult result = new DeleteTopicsResult(TopicCollection.TopicAttribute.TOPIC_ID);
        result.setTopicIdFutures(topicIdFutures);
        return result;
    }

    /**
     * @return the attribute used to identify topics in the futures map
     */
    public TopicCollection.TopicAttribute attribute() {
        return attribute;
    }

    /**
     * Return a map from topic names to futures which can be used to check the status of
     * individual deletions.
     * @throws UnsupportedOperationException if the collection's attribute was not TOPIC_NAME
     */
    public Map<String, KafkaFuture<Void>> topicNameValues() {
        if (attribute != TopicCollection.TopicAttribute.TOPIC_NAME)
            throw new UnsupportedOperationException("Can not get topic name values unless TopicAttribute is TOPIC_NAME");
        return nameFutures;
    }


    /**
     * Return a map from topic names to futures which can be used to check the status of
     * individual deletions.
     * @throws UnsupportedOperationException if the collection's attribute was not TOPIC_ID
     */
    public Map<Uuid, KafkaFuture<Void>> topicIdValues() {
        if (attribute != TopicCollection.TopicAttribute.TOPIC_ID)
            throw new UnsupportedOperationException("Can not get topic ID values unless TopicAttribute is TOPIC_ID");
        return topicIdFutures;
    }

    @Deprecated
    /**
     * Return a map from topic names to futures which can be used to check the status of
     * individual deletions.
     */
    public Map<String, KafkaFuture<Void>> values() {
        if (attribute != TopicCollection.TopicAttribute.TOPIC_NAME)
            throw new UnsupportedOperationException("Can not get topic name values unless TopicAttribute is TOPIC_NAME");
        return nameFutures;
    }

    /**
     * Return a future which succeeds only if all the topic deletions succeed.
     * @throws IllegalStateException if the topic attribute is not supported.
     */
    public KafkaFuture<Void> all() {
        KafkaFuture<Void> future;
        switch (attribute) {
            case TOPIC_ID:
                future = KafkaFuture.allOf(topicIdFutures.values().toArray(new KafkaFuture[0]));
                break;
            case TOPIC_NAME:
                future = KafkaFuture.allOf(nameFutures.values().toArray(new KafkaFuture[0]));
                break;
            default:
                throw new IllegalStateException("TopicAttribute" + attribute + " did not match the supported attributes.");
        }
        return future;
    }
}
