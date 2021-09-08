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

package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Represents the topics in the metadata image.
 *
 * This class is thread-safe.
 */
public final class TopicsImage {
    public static final TopicsImage EMPTY =
        new TopicsImage(Collections.emptyMap(), Collections.emptyMap());

    private final Map<Uuid, TopicImage> topicsById;
    private final Map<String, TopicImage> topicsByName;

    public TopicsImage(Map<Uuid, TopicImage> topicsById,
                       Map<String, TopicImage> topicsByName) {
        this.topicsById = Collections.unmodifiableMap(topicsById);
        this.topicsByName = Collections.unmodifiableMap(topicsByName);
    }

    public boolean isEmpty() {
        return topicsById.isEmpty() && topicsByName.isEmpty();
    }

    public Map<Uuid, TopicImage> topicsById() {
        return topicsById;
    }

    public Map<String, TopicImage> topicsByName() {
        return topicsByName;
    }

    public PartitionRegistration getPartition(Uuid id, int partitionId) {
        TopicImage topicImage = topicsById.get(id);
        if (topicImage == null) return null;
        return topicImage.partitions().get(partitionId);
    }

    public TopicImage getTopic(Uuid id) {
        return topicsById.get(id);
    }

    public TopicImage getTopic(String name) {
        return topicsByName.get(name);
    }

    public void write(Consumer<List<ApiMessageAndVersion>> out) {
        for (TopicImage topicImage : topicsById.values()) {
            topicImage.write(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TopicsImage)) return false;
        TopicsImage other = (TopicsImage) o;
        return topicsById.equals(other.topicsById) &&
            topicsByName.equals(other.topicsByName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicsById, topicsByName);
    }

    /**
     * Expose a view of this TopicsImage as a map from topic names to IDs.
     *
     * Like TopicsImage itself, this map is immutable.
     */
    public Map<String, Uuid> topicNameToIdView() {
        return new TopicNameToIdMap();
    }

    class TopicNameToIdMap extends AbstractMap<String, Uuid> {
        private final TopicNameToIdMapEntrySet set = new TopicNameToIdMapEntrySet();

        @Override
        public boolean containsKey(Object key) {
            return topicsByName.containsKey(key);
        }

        @Override
        public Uuid get(Object key) {
            TopicImage image = topicsByName.get(key);
            if (image == null) return null;
            return image.id();
        }

        @Override
        public Set<Entry<String, Uuid>> entrySet() {
            return set;
        }
    }

    class TopicNameToIdMapEntrySet extends AbstractSet<Entry<String, Uuid>> {
        @Override
        public Iterator<Entry<String, Uuid>> iterator() {
            return new TopicNameToIdMapEntrySetIterator(topicsByName.entrySet().iterator());
        }

        @SuppressWarnings("rawtypes")
        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Entry)) return false;
            Entry other = (Entry) o;
            TopicImage image = topicsByName.get(other.getKey());
            if (image == null) return false;
            return image.id().equals(other.getValue());
        }

        @Override
        public int size() {
            return topicsByName.size();
        }
    }

    static class TopicNameToIdMapEntrySetIterator implements Iterator<Entry<String, Uuid>> {
        private final Iterator<Entry<String, TopicImage>> iterator;

        TopicNameToIdMapEntrySetIterator(Iterator<Entry<String, TopicImage>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Entry<String, Uuid> next() {
            Entry<String, TopicImage> entry = iterator.next();
            return new SimpleImmutableEntry<>(entry.getKey(), entry.getValue().id());
        }
    }

    /**
     * Expose a view of this TopicsImage as a map from IDs to names.
     *
     * Like TopicsImage itself, this map is immutable.
     */
    public Map<Uuid, String> topicIdToNameView() {
        return new TopicIdToNameMap();
    }

    class TopicIdToNameMap extends AbstractMap<Uuid, String> {
        private final TopicIdToNameMapEntrySet set = new TopicIdToNameMapEntrySet();

        @Override
        public boolean containsKey(Object key) {
            return topicsById.containsKey(key);
        }

        @Override
        public String get(Object key) {
            TopicImage image = topicsById.get(key);
            if (image == null) return null;
            return image.name();
        }

        @Override
        public Set<Entry<Uuid, String>> entrySet() {
            return set;
        }
    }

    class TopicIdToNameMapEntrySet extends AbstractSet<Entry<Uuid, String>> {
        @Override
        public Iterator<Entry<Uuid, String>> iterator() {
            return new TopicIdToNameEntrySetIterator(topicsById.entrySet().iterator());
        }

        @SuppressWarnings("rawtypes")
        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Entry)) return false;
            Entry other = (Entry) o;
            TopicImage image = topicsById.get(other.getKey());
            if (image == null) return false;
            return image.name().equals(other.getValue());
        }

        @Override
        public int size() {
            return topicsById.size();
        }
    }

    static class TopicIdToNameEntrySetIterator implements Iterator<Entry<Uuid, String>> {
        private final Iterator<Entry<Uuid, TopicImage>> iterator;

        TopicIdToNameEntrySetIterator(Iterator<Entry<Uuid, TopicImage>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Entry<Uuid, String> next() {
            Entry<Uuid, TopicImage> entry = iterator.next();
            return new SimpleImmutableEntry<>(entry.getKey(), entry.getValue().name());
        }
    }

    @Override
    public String toString() {
        return "TopicsImage(topicsById=" + topicsById.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
            ", topicsByName=" + topicsByName.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
            ")";
    }
}
