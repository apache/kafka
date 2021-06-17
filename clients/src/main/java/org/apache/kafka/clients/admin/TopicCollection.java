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

import org.apache.kafka.common.Uuid;

import java.util.Collection;

public class TopicCollection {

    private Collection<String> topicNames;
    private Collection<Uuid> topicIds;
    private final TopicAttribute attribute;

    public enum TopicAttribute {
        TOPIC_NAME, TOPIC_ID
    }

    private TopicCollection(TopicAttribute attribute) {
        this.attribute = attribute;
    }

    private void setTopicNames(Collection<String> topicNames) {
        this.topicNames = topicNames;
    }

    private void setTopicIds(Collection<Uuid> topicIds) {
        this.topicIds = topicIds;
    }

    /**
     * @throws UnsupportedOperationException if the collection's attribute was not TOPIC_NAME
     * @return A collection of topic names
     */
    public Collection<String> topicNames() {
        if (attribute != TopicAttribute.TOPIC_NAME)
            throw new UnsupportedOperationException("Can not get topic names unless TopicAttribute is TOPIC_NAME");
        return topicNames;
    }

    /**
     * @throws UnsupportedOperationException if the collection's attribute was not TOPIC_ID
     * @return A collection of topic IDs
     */
    public Collection<Uuid> topicIds() {
        if (attribute != TopicAttribute.TOPIC_ID)
            throw new UnsupportedOperationException("Can not get topic IDs unless TopicAttribute is TOPIC_ID");
        return topicIds;
    }

    /**
     * @return the attribute used to identify topics in this collection
     */
    public TopicAttribute attribute() {
        return attribute;
    }

    /**
     * Static method to create a TopicCollection comprised of topic IDs. Sets the topic attribute to TOPIC_ID
     * @return the TopicCollection created with the given topicIds
     */
    public static TopicCollection ofTopicIds(Collection<Uuid> topicIds) {
        TopicCollection idCollection = new TopicCollection(TopicAttribute.TOPIC_ID);
        idCollection.setTopicIds(topicIds);
        return idCollection;
    }

    /**
     * Static method to create a TopicCollection comprised of topic names. Sets the topic attribute to TOPIC_NAME
     * @return the TopicCollection created with the given topicNames
     */
    public static TopicCollection ofTopicNames(Collection<String> topicNames) {
        TopicCollection nameCollection = new TopicCollection(TopicAttribute.TOPIC_NAME);
        nameCollection.setTopicNames(topicNames);
        return nameCollection;
    }
}