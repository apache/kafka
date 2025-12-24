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
package org.apache.kafka.coordinator.group.modern.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopicRegexResolverTest {

    private final Logger log = LoggerFactory.getLogger(TopicRegexResolverTest.class);

    @Test
    public void testBasicMatching() {
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "foo", 10)
            .addTopic(Uuid.randomUuid(), "bar", 10)
            .addTopic(Uuid.randomUuid(), "baz", 10)
            .addTopic(Uuid.randomUuid(), "qux", 10)
            .buildCoordinatorMetadataImage();

        Time time = new MockTime(0L, 0L, 0L);

        TopicRegexResolver resolver = new TopicRegexResolver(Optional::empty, time);

        var result = resolver.resolveRegularExpressions(
            null,
            "group-1",
            log,
            image,
            Set.of("ba.*")
        );

        var resolved = result.get("ba.*");

        assertEquals(Set.of("bar", "baz"), resolved.topics());
        assertEquals(image.version(), resolved.version());
        assertEquals(0L, resolved.timestamp());
    }

    @Test
    public void testInvalidRegexIgnored() {
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "foo", 10)
            .addTopic(Uuid.randomUuid(), "bar", 10)
            .buildCoordinatorMetadataImage();

        Time time = new MockTime(5L, 0L, 0L);

        TopicRegexResolver resolver = new TopicRegexResolver(Optional::empty, time);

        var result = resolver.resolveRegularExpressions(
            null,
            "group-2",
            log,
            image,
            Set.of("a.*")
        );

        var resolved = result.get("a.*");

        assertTrue(resolved.topics().isEmpty());
        assertEquals(image.version(), resolved.version());
        assertEquals(5L, resolved.timestamp());
    }

    @Test
    public void testAuthorizationFiltering() {
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "allow1", 10)
            .addTopic(Uuid.randomUuid(), "deny1", 10)
            .addTopic(Uuid.randomUuid(), "allow2", 10)
            .buildCoordinatorMetadataImage();

        Time time = new MockTime(10L, 0L, 0L);

        Authorizer authorizer = mock(Authorizer.class);
        when(authorizer.authorize(any(), any())).thenAnswer(invocation -> {
            List<Action> actions = invocation.getArgument(1);
            var results = new ArrayList<>(actions.size());
            for (Action action : actions) {
                String topic = action.resourcePattern().name();
                results.add("deny1".equals(topic) ? AuthorizationResult.DENIED : AuthorizationResult.ALLOWED);
            }
            return results;
        });

        var plugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");

        TopicRegexResolver resolver = new TopicRegexResolver(() -> Optional.of(plugin), time);

        var result = resolver.resolveRegularExpressions(
            null,
            "group-3",
            log,
            image,
            Set.of("a.*", "d.*")
        );

        var resolved = result.get("a.*");

        assertEquals(Set.of("allow1", "allow2"), resolved.topics());
        assertEquals(image.version(), resolved.version());
        assertEquals(10L, resolved.timestamp());

        resolved = result.get("d.*");
        assertTrue(resolved.topics().isEmpty());
        assertEquals(image.version(), resolved.version());
        assertEquals(10L, resolved.timestamp());
    }
}
