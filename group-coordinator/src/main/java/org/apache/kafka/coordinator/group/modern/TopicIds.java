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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * TopicIds is initialized with topic names (String) but exposes a Set of topic ids (Uuid) to the
 * user and performs the conversion lazily with TopicsImage.
 */
public class TopicIds implements Set<Uuid> {
    private final Set<String> topicNames;
    private final TopicsImage image;

    public TopicIds(
        Set<String> topicNames,
        TopicsImage image
    ) {
        this.topicNames = Objects.requireNonNull(topicNames);
        this.image = Objects.requireNonNull(image);
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
            TopicImage topicImage = image.getTopic(topicId);
            if (topicImage == null) return false;
            return topicNames.contains(topicImage.name());
        }
        return false;
    }

    private static class TopicIdIterator implements Iterator<Uuid> {
        final Iterator<String> iterator;
        final TopicsImage image;
        private Uuid next = null;

        private TopicIdIterator(
            Iterator<String> iterator,
            TopicsImage image
        ) {
            this.iterator = Objects.requireNonNull(iterator);
            this.image = Objects.requireNonNull(image);
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
                TopicImage topicImage = image.getTopic(next);
                if (topicImage != null) {
                    result = topicImage.id();
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
        return new TopicIdIterator(topicNames.iterator(), image);
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
        return Objects.equals(image, uuids.image);
    }

    @Override
    public int hashCode() {
        int result = topicNames.hashCode();
        result = 31 * result + image.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TopicIds(topicNames=" + topicNames +
            ", image=" + image +
            ')';
    }
}
