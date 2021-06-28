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

import java.util.Collection;

/**
 * A class used to represent a collection of topics. This collection may define topics by topic name
 * or topic ID. Subclassing this class beyond the classes provided here is not supported.
 */
public abstract class TopicCollection {

    private TopicCollection() {}

    /**
     * A class used to represent a collection of topics defined by their topic ID.
     * Subclassing this class beyond the classes provided here is not supported.
     */
    public static class TopicIdCollection extends TopicCollection {
        private final Collection<Uuid> topicIds;

        public TopicIdCollection(Collection<Uuid> topicIds) {
            super();
            this.topicIds = topicIds;
        }

        /**
         * @return A collection of topic IDs
         */
        public Collection<Uuid> topicIds() {
            return topicIds;
        }
    }

    /**
     * A class used to represent a collection of topics defined by their topic name.
     * Subclassing this class beyond the classes provided here is not supported.
     */
    public static class TopicNameCollection extends TopicCollection {
        private final Collection<String> topicNames;

        public TopicNameCollection(Collection<String> topicNames) {
            super();
            this.topicNames = topicNames;
        }

        /**
         * @return A collection of topic names
         */
        public Collection<String> topicNames() {
            return topicNames;
        }
    }
}