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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PositionTest {

    @Test
    public void shouldCreateFromMap() {
        final Map<String, Map<Integer, Long>> map = mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 5L))),
            mkEntry("topic1", mkMap(
                mkEntry(0, 5L),
                mkEntry(7, 0L)
            ))
        );

        final Position position = Position.fromMap(map);
        assertThat(position.getTopics(), equalTo(mkSet("topic", "topic1")));
        assertThat(position.getPartitionPositions("topic"), equalTo(mkMap(mkEntry(0, 5L))));

        // Should be a copy of the constructor map

        map.get("topic1").put(99, 99L);

        // so the position is still the original one
        assertThat(position.getPartitionPositions("topic1"), equalTo(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L)
        )));
    }

    @Test
    public void shouldCreateFromNullMap() {
        final Position position = Position.fromMap(null);
        assertThat(position.getTopics(), equalTo(Collections.emptySet()));
    }

    @Test
    public void shouldMerge() {
        final Position position = Position.fromMap(mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 5L))),
            mkEntry("topic1", mkMap(
                mkEntry(0, 5L),
                mkEntry(7, 0L)
            ))
        ));

        final Position position1 = Position.fromMap(mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 7L))), // update offset
            mkEntry("topic1", mkMap(mkEntry(8, 1L))), // add partition
            mkEntry("topic2", mkMap(mkEntry(9, 5L))) // add topic
        ));

        final Position merged = position.merge(position1);

        assertThat(merged.getTopics(), equalTo(mkSet("topic", "topic1", "topic2")));
        assertThat(merged.getPartitionPositions("topic"), equalTo(mkMap(mkEntry(0, 7L))));
        assertThat(merged.getPartitionPositions("topic1"), equalTo(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L),
            mkEntry(8, 1L)
        )));
        assertThat(merged.getPartitionPositions("topic2"), equalTo(mkMap(mkEntry(9, 5L))));
    }

    @Test
    public void shouldUpdateComponentMonotonically() {
        final Position position = Position.emptyPosition();
        position.withComponent("topic", 3, 5L);
        position.withComponent("topic", 3, 4L);
        assertThat(position.getPartitionPositions("topic").get(3), equalTo(5L));
        position.withComponent("topic", 3, 6L);
        assertThat(position.getPartitionPositions("topic").get(3), equalTo(6L));
    }

    @Test
    public void shouldCopy() {
        final Position position = Position.fromMap(mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 5L))),
            mkEntry("topic1", mkMap(
                mkEntry(0, 5L),
                mkEntry(7, 0L)
            ))
        ));

        final Position copy = position.copy();

        // mutate original
        position.withComponent("topic", 0, 6L);
        position.withComponent("topic1", 8, 1L);
        position.withComponent("topic2", 2, 4L);

        // copy has not changed
        assertThat(copy.getTopics(), equalTo(mkSet("topic", "topic1")));
        assertThat(copy.getPartitionPositions("topic"), equalTo(mkMap(mkEntry(0, 5L))));
        assertThat(copy.getPartitionPositions("topic1"), equalTo(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L)
        )));

        // original has changed
        assertThat(position.getTopics(), equalTo(mkSet("topic", "topic1", "topic2")));
        assertThat(position.getPartitionPositions("topic"), equalTo(mkMap(mkEntry(0, 6L))));
        assertThat(position.getPartitionPositions("topic1"), equalTo(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L),
            mkEntry(8, 1L)
        )));
        assertThat(position.getPartitionPositions("topic2"), equalTo(mkMap(mkEntry(2, 4L))));
    }

    @Test
    public void shouldMergeNull() {
        final Position position = Position.fromMap(mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 5L))),
            mkEntry("topic1", mkMap(
                mkEntry(0, 5L),
                mkEntry(7, 0L)
            ))
        ));

        final Position merged = position.merge(null);

        assertThat(merged.getTopics(), equalTo(mkSet("topic", "topic1")));
        assertThat(merged.getPartitionPositions("topic"), equalTo(mkMap(mkEntry(0, 5L))));
        assertThat(merged.getPartitionPositions("topic1"), equalTo(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L)
        )));
    }

    @Test
    public void shouldMatchOnEqual() {
        final Position position1 = Position.emptyPosition();
        final Position position2 = Position.emptyPosition();
        position1.withComponent("topic1", 0, 1);
        position2.withComponent("topic1", 0, 1);

        position1.withComponent("topic1", 1, 2);
        position2.withComponent("topic1", 1, 2);

        position1.withComponent("topic1", 2, 1);
        position2.withComponent("topic1", 2, 1);

        position1.withComponent("topic2", 0, 0);
        position2.withComponent("topic2", 0, 0);

        assertEquals(position1, position2);
    }

    @Test
    public void shouldNotMatchOnUnEqual() {
        final Position position1 = Position.emptyPosition();
        final Position position2 = Position.emptyPosition();
        position1.withComponent("topic1", 0, 1);
        position2.withComponent("topic1", 0, 1);

        position1.withComponent("topic1", 1, 2);

        position1.withComponent("topic1", 2, 1);
        position2.withComponent("topic1", 2, 1);

        position1.withComponent("topic2", 0, 0);
        position2.withComponent("topic2", 0, 0);

        assertNotEquals(position1, position2);
    }

    @Test
    public void shouldNotMatchNull() {
        final Position position = Position.emptyPosition();
        assertNotEquals(position, null);
    }

    @Test
    public void shouldMatchSelf() {
        final Position position = Position.emptyPosition();
        assertEquals(position, position);
    }

    @Test
    public void shouldNotHash() {
        final Position position = Position.emptyPosition();
        assertThrows(UnsupportedOperationException.class, position::hashCode);

        // going overboard...
        final HashSet<Position> set = new HashSet<>();
        assertThrows(UnsupportedOperationException.class, () -> set.add(position));

        final HashMap<Position, Integer> map = new HashMap<>();
        assertThrows(UnsupportedOperationException.class, () -> map.put(position, 5));
    }
}
