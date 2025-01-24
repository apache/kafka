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
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.image.TopicsImage;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicIdsTest {

    @Test
    public void testTopicNamesCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new TopicIds(null, TopicsImage.EMPTY));
    }

    @Test
    public void testTopicsImageCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new TopicIds(Collections.emptySet(), (TopicsImage) null));
    }

    @Test
    public void testTopicResolverCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new TopicIds(Collections.emptySet(), (TopicIds.TopicResolver) null));
    }

    @Test
    public void testSize() {
        Set<String> topicNames = Set.of("foo", "bar", "baz");
        Set<Uuid> topicIds = new TopicIds(topicNames, TopicsImage.EMPTY);
        assertEquals(topicNames.size(), topicIds.size());
    }

    @Test
    public void testIsEmpty() {
        Set<String> topicNames = Collections.emptySet();
        Set<Uuid> topicIds = new TopicIds(topicNames, TopicsImage.EMPTY);
        assertEquals(topicNames.size(), topicIds.size());
    }

    @Test
    public void testContains() {
        Uuid fooUuid = Uuid.randomUuid();
        Uuid barUuid = Uuid.randomUuid();
        Uuid bazUuid = Uuid.randomUuid();
        Uuid quxUuid = Uuid.randomUuid();
        TopicsImage topicsImage = new MetadataImageBuilder()
            .addTopic(fooUuid, "foo", 3)
            .addTopic(barUuid, "bar", 3)
            .addTopic(bazUuid, "qux", 3)
            .build()
            .topics();

        Set<Uuid> topicIds = new TopicIds(Set.of("foo", "bar", "baz"), topicsImage);

        assertTrue(topicIds.contains(fooUuid));
        assertTrue(topicIds.contains(barUuid));
        assertFalse(topicIds.contains(bazUuid));
        assertFalse(topicIds.contains(quxUuid));
    }

    @Test
    public void testContainsAll() {
        Uuid fooUuid = Uuid.randomUuid();
        Uuid barUuid = Uuid.randomUuid();
        Uuid bazUuid = Uuid.randomUuid();
        Uuid quxUuid = Uuid.randomUuid();
        TopicsImage topicsImage = new MetadataImageBuilder()
            .addTopic(fooUuid, "foo", 3)
            .addTopic(barUuid, "bar", 3)
            .addTopic(bazUuid, "baz", 3)
            .addTopic(quxUuid, "qux", 3)
            .build()
            .topics();

        Set<Uuid> topicIds = new TopicIds(Set.of("foo", "bar", "baz", "qux"), topicsImage);

        assertTrue(topicIds.contains(fooUuid));
        assertTrue(topicIds.contains(barUuid));
        assertTrue(topicIds.contains(bazUuid));
        assertTrue(topicIds.contains(quxUuid));
        assertTrue(topicIds.containsAll(Set.of(fooUuid, barUuid, bazUuid, quxUuid)));
    }

    @Test
    public void testContainsAllOneTopicConversionFails() {
        // topic 'qux' only exists as topic name.
        Uuid fooUuid = Uuid.randomUuid();
        Uuid barUuid = Uuid.randomUuid();
        Uuid bazUuid = Uuid.randomUuid();
        Uuid quxUuid = Uuid.randomUuid();
        TopicsImage topicsImage = new MetadataImageBuilder()
            .addTopic(fooUuid, "foo", 3)
            .addTopic(barUuid, "bar", 3)
            .addTopic(bazUuid, "baz", 3)
            .build()
            .topics();

        Set<Uuid> topicIds = new TopicIds(Set.of("foo", "bar", "baz", "qux"), topicsImage);

        assertTrue(topicIds.contains(fooUuid));
        assertTrue(topicIds.contains(barUuid));
        assertTrue(topicIds.contains(bazUuid));
        assertTrue(topicIds.containsAll(Set.of(fooUuid, barUuid, bazUuid)));
        assertFalse(topicIds.containsAll(Set.of(fooUuid, barUuid, bazUuid, quxUuid)));
    }

    @Test
    public void testIterator() {
        Uuid fooUuid = Uuid.randomUuid();
        Uuid barUuid = Uuid.randomUuid();
        Uuid bazUuid = Uuid.randomUuid();
        Uuid quxUuid = Uuid.randomUuid();
        TopicsImage topicsImage = new MetadataImageBuilder()
            .addTopic(fooUuid, "foo", 3)
            .addTopic(barUuid, "bar", 3)
            .addTopic(bazUuid, "baz", 3)
            .addTopic(quxUuid, "qux", 3)
            .build()
            .topics();

        Set<Uuid> topicIds = new TopicIds(Set.of("foo", "bar", "baz", "qux"), topicsImage);
        Set<Uuid> expectedIds = Set.of(fooUuid, barUuid, bazUuid, quxUuid);
        Set<Uuid> actualIds = new HashSet<>(topicIds);

        assertEquals(expectedIds, actualIds);
    }

    @Test
    public void testIteratorOneTopicConversionFails() {
        // topic 'qux' only exists as topic id.
        // topic 'quux' only exists as topic name.
        Uuid fooUuid = Uuid.randomUuid();
        Uuid barUuid = Uuid.randomUuid();
        Uuid bazUuid = Uuid.randomUuid();
        Uuid qux = Uuid.randomUuid();
        TopicsImage topicsImage = new MetadataImageBuilder()
            .addTopic(fooUuid, "foo", 3)
            .addTopic(barUuid, "bar", 3)
            .addTopic(bazUuid, "baz", 3)
            .addTopic(qux, "qux", 3)
            .build()
            .topics();

        Set<Uuid> topicIds = new TopicIds(Set.of("foo", "bar", "baz", "quux"), topicsImage);
        Set<Uuid> expectedIds = Set.of(fooUuid, barUuid, bazUuid);
        Set<Uuid> actualIds = new HashSet<>(topicIds);

        assertEquals(expectedIds, actualIds);
    }

    @Test
    public void testEquals() {
        Uuid topicId = Uuid.randomUuid();
        TopicIds topicIds1 = new TopicIds(Collections.singleton("topic"),
            new MetadataImageBuilder()
                .addTopic(topicId, "topicId", 3)
                .build()
                .topics()
        );

        TopicIds topicIds2 = new TopicIds(Collections.singleton("topic"),
            new MetadataImageBuilder()
                .addTopic(topicId, "topicId", 3)
                .build()
                .topics()
        );

        assertEquals(topicIds1, topicIds2);
    }
}
