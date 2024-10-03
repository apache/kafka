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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * TopicIds is initialized with topic names (String) but exposes a Set of topic ids (Uuid) to the
 * user and performs the conversion lazily with a TopicResolver backed by a TopicsImage.
 */
public class TopicIds implements Set<Uuid> {
    /**
     * Converts between topic ids (Uuids) and topic names (Strings).
     */
    public interface TopicResolver {
        /**
         * @return The TopicsImage used by the resolver.
         */
        TopicsImage image();

        /**
         * Converts a topic id to a topic name.
         *
         * @param id The topic id.
         * @return The topic name for the given topic id, or null if the topic does not exist.
         */
        String name(Uuid id);

        /**
         * Converts a topic name to a topic id.
         *
         * @param name The topic name.
         * @return The topic id for the given topic name, or null if the topic does not exist.
         */
        Uuid id(String name);

        /**
         * Clears any cached data.
         *
         * Used for benchmarking purposes.
         */
        void clear();
    }

    /**
     * A TopicResolver without any caching.
     */
    public static class DefaultTopicResolver implements TopicResolver {
        private final TopicsImage image;

        public DefaultTopicResolver(
            TopicsImage image
        ) {
            this.image = Objects.requireNonNull(image);
        }

        @Override
        public final TopicsImage image() {
            return image;
        }

        @Override
        public String name(Uuid id) {
            TopicImage topic = image.getTopic(id);
            if (topic == null) return null;
            return topic.name();
        }

        @Override
        public Uuid id(String name) {
            TopicImage topic = image.getTopic(name);
            if (topic == null) return null;
            return topic.id();
        }

        @Override
        public void clear() {}

        @Override
        public String toString() {
            return "DefaultTopicResolver(image=" + image + ")";
        }
    }

    /**
     * A TopicResolver that caches results.
     *
     * This cache is expected to be short-lived and only used within a single
     * TargetAssignmentBuilder.build() call.
     */
    public static class CachedTopicResolver implements TopicResolver {
        private final TopicsImage image;

        private final Map<String, Uuid> topicIds = new HashMap<>();
        private final Map<Uuid, String> topicNames = new HashMap<>();

        public CachedTopicResolver(
            TopicsImage image
        ) {
            this.image = Objects.requireNonNull(image);
        }

        @Override
        public final TopicsImage image() {
            return image;
        }

        @Override
        public String name(Uuid id) {
            return topicNames.computeIfAbsent(id, __ -> {
                TopicImage topic = image.getTopic(id);
                if (topic == null) return null;
                return topic.name();
            });
        }

        @Override
        public Uuid id(String name) {
            return topicIds.computeIfAbsent(name, __ -> {
                TopicImage topic = image.getTopic(name);
                if (topic == null) return null;
                return topic.id();
            });
        }

        @Override
        public void clear() {
            this.topicNames.clear();
            this.topicIds.clear();
        }

        @Override
        public String toString() {
            return "CachedTopicResolver(image=" + image + ")";
        }
    }

    private final Set<String> topicNames;
    private final TopicResolver resolver;

    public TopicIds(
        Set<String> topicNames,
        TopicsImage image
    ) {
        this.topicNames = Objects.requireNonNull(topicNames);
        this.resolver = new DefaultTopicResolver(image);
    }

    public TopicIds(
        Set<String> topicNames,
        TopicResolver resolver
    ) {
        this.topicNames = Objects.requireNonNull(topicNames);
        this.resolver = Objects.requireNonNull(resolver);
    }

    @Override
    public int size() {
        return topicNames.size();
    }

    @Override
    public boolean isEmpty() {
        return topicNames.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof Uuid) {
            Uuid topicId = (Uuid) o;
            String topicName = resolver.name(topicId);
            if (topicName == null) return false;
            return topicNames.contains(topicName);
        }
        return false;
    }

    private static class TopicIdIterator implements Iterator<Uuid> {
        final Iterator<String> iterator;
        final TopicResolver resolver;
        private Uuid next = null;

        private TopicIdIterator(
            Iterator<String> iterator,
            TopicResolver resolver
        ) {
            this.iterator = Objects.requireNonNull(iterator);
            this.resolver = Objects.requireNonNull(resolver);
        }

        @Override
        public boolean hasNext() {
            if (next != null) return true;
            Uuid result = null;
            do {
                if (!iterator.hasNext()) {
                    return false;
                }
                String next = iterator.next();
                Uuid topicId = resolver.id(next);
                if (topicId != null) {
                    result = topicId;
                }
            } while (result == null);
            next = result;
            return true;
        }

        @Override
        public Uuid next() {
            if (!hasNext()) throw new NoSuchElementException();
            Uuid result = next;
            next = null;
            return result;
        }
    }

    @Override
    public Iterator<Uuid> iterator() {
        return new TopicIdIterator(topicNames.iterator(), resolver);
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Uuid o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection c) {
        for (Object o : c) {
            if (!contains(o)) return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicIds uuids = (TopicIds) o;

        if (!Objects.equals(topicNames, uuids.topicNames)) return false;
        return Objects.equals(resolver.image(), uuids.resolver.image());
    }

    @Override
    public int hashCode() {
        int result = topicNames.hashCode();
        result = 31 * result + resolver.image().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TopicIds(topicNames=" + topicNames +
            ", resolver=" + resolver +
            ')';
    }
}
