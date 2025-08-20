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

package org.apache.kafka.streams.query;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PositionBoundTest {

    @Test
    public void shouldCopyPosition() {
        final Position position = Position.emptyPosition();
        final PositionBound positionBound = PositionBound.at(position);
        position.withComponent("topic", 1, 2L);

        assertThat(position.getTopics(), equalTo(Set.of("topic")));
        assertThat(positionBound.position().getTopics(), empty());
    }

    @Test
    public void unboundedShouldBeUnbounded() {
        final PositionBound bound = PositionBound.unbounded();
        assertTrue(bound.isUnbounded());
    }

    @Test
    public void unboundedShouldReturnEmptyPosition() {
        final PositionBound bound = PositionBound.unbounded();
        assertThat(bound.position(), equalTo(Position.emptyPosition()));
    }

    @Test
    public void shouldEqualPosition() {
        final PositionBound bound1 = PositionBound.at(Position.emptyPosition());
        final PositionBound bound2 = PositionBound.at(Position.emptyPosition());
        assertEquals(bound1, bound2);
    }

    @Test
    public void shouldEqualUnbounded() {
        final PositionBound bound1 = PositionBound.unbounded();
        final PositionBound bound2 = PositionBound.unbounded();
        assertEquals(bound1, bound2);
    }

    @Test
    public void shouldEqualSelf() {
        final PositionBound bound1 = PositionBound.at(Position.emptyPosition());
        assertEquals(bound1, bound1);
    }

    @Test
    public void shouldNotEqualNull() {
        final PositionBound bound1 = PositionBound.at(Position.emptyPosition());
        assertNotEquals(bound1, null);
    }

    @Test
    public void shouldNotHash() {
        final PositionBound bound = PositionBound.at(Position.emptyPosition());
        assertThrows(UnsupportedOperationException.class, bound::hashCode);

        // going overboard...
        final HashSet<PositionBound> set = new HashSet<>();
        assertThrows(UnsupportedOperationException.class, () -> set.add(bound));

        final HashMap<PositionBound, Integer> map = new HashMap<>();
        assertThrows(UnsupportedOperationException.class, () -> map.put(bound, 5));
    }
}
