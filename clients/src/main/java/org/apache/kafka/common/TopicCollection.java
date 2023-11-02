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
package org.apache.kafka.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * A class used to represent a collection of topics. This collection may define topics by name or ID.
 */
public abstract class TopicCollection {

    private TopicCollection() {}

    /**
     * @return a collection of topics defined by topic ID
     */
    public static TopicIdCollection ofTopicIds(Collection<Uuid> topics) {
        return new TopicIdCollection(topics);
    }

    /**
     * @return a collection of topics defined by topic name
     */
    public static TopicNameCollection ofTopicNames(Collection<String> topics) {
        return new TopicNameCollection(topics);
    }

    /**
     * A class used to represent a collection of topics defined by their topic ID.
     * Subclassing this class beyond the classes provided here is not supported.
     */
    public static class TopicIdCollection extends TopicCollection {
        private final Collection<Uuid> topicIds;

        private TopicIdCollection(Collection<Uuid> topicIds) {
            this.topicIds = new ArrayList<>(topicIds);
        }

        /**
         * @return A collection of topic IDs
         */
        public Collection<Uuid> topicIds() {
            return Collections.unmodifiableCollection(topicIds);
        }
    }

    /**
     * A class used to represent a collection of topics defined by their topic name.
     * Subclassing this class beyond the classes provided here is not supported.
     */
    public static class TopicNameCollection extends TopicCollection {
        private final Collection<String> topicNames;

        private TopicNameCollection(Collection<String> topicNames) {
            this.topicNames = new ArrayList<>(topicNames);
        }

        /**
         * @return A collection of topic names
         */
        public Collection<String> topicNames() {
            return Collections.unmodifiableCollection(topicNames);
        }
    }
}
