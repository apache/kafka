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

import org.apache.kafka.common.errors.InvalidTopicException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Encapsulates the resolution of a topic name from its ID, or its ID from its name, using a local
 * bidirectional mapping. This resolver assumes there is a bijection between topic IDs and topic names.
 * <p></p>
 * Note that this class intends to be used for the (reverse) lookup of topic IDs/names, but
 * not to characterize the set of topics which are known by a client. Use the
 * {@link org.apache.kafka.clients.MetadataCache} for that purpose.
 */
public class TopicResolver {
    private final Map<String, Uuid> topicIds;
    private final Map<Uuid, String> topicNames;

    /**
     * A resolver which universe of topic ids and names is captured from the input map. The reverse association
     * between a topic ID and a topic name is computed by this method. If there are more than one topic name
     * resolving to the same topic ID, an {@link InvalidTopicException} is thrown.
     */
    public static TopicResolver fromTopicIds(Map<String, Uuid> topicIds) {
        Map<Uuid, String> topicNames = new HashMap<>(topicIds.size());

        for (Map.Entry<String, Uuid> e: topicIds.entrySet()) {
            String conflicting = topicNames.putIfAbsent(e.getValue(), e.getKey());
            if (conflicting != null) {
                throw new IllegalStateException(
                        "Topic " + e.getKey() + " shares the same ID " + e.getValue() + " as topic " + conflicting);
            }
        }

        return new TopicResolver(topicIds, topicNames);
    }

    /**
     * A resolver which acts as a wrapper around the input mapping of topic ids from/to topic names.
     * No validation is performed about the consistency of the mapping. This method is to be preferred
     * when the copy of the input maps needs to be avoided.
     */
    public static TopicResolver wrap(Map<String, Uuid> topicIds, Map<Uuid, String> topicNames) {
        return new TopicResolver(topicIds, topicNames);
    }

    /**
     * A resolver with no existing mapping between any topic name and id.
     */
    public static TopicResolver emptyResolver() {
        return fromTopicIds(Collections.emptyMap());
    }

    private TopicResolver(Map<String, Uuid> topicIds, Map<Uuid, String> topicNames) {
        this.topicIds = Collections.unmodifiableMap(topicIds);
        this.topicNames = Collections.unmodifiableMap(topicNames);
    }

    /**
     * Returns the ID of the topic with the given name, if that association exists.
     */
    public Optional<Uuid> getTopicId(String name) {
        return Optional.ofNullable(topicIds.get(name));
    }

    /**
     * Returns the name of the topic corresponding to the given ID, if that association exists.
     */
    public Optional<String> getTopicName(Uuid uuid) {
        return Optional.ofNullable(topicNames.get(uuid));
    }
}
