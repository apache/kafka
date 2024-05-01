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
package org.apache.kafka.raft.internals;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final public class TreeMapLogHistoryTest {
    @Test
    void testEmpty() {
        TreeMapLogHistory<String> history = new TreeMapLogHistory<>();
        assertEquals(Optional.empty(), history.valueAtOrBefore(100));
        assertEquals(Optional.empty(), history.lastEntry());
    }

    @Test
    void testAddAt() {
        TreeMapLogHistory<String> history = new TreeMapLogHistory<>();
        assertThrows(IllegalArgumentException.class, () -> history.addAt(-1, ""));
        assertEquals(Optional.empty(), history.lastEntry());

        history.addAt(100, "100");
        assertThrows(IllegalArgumentException.class, () -> history.addAt(99, ""));
        assertThrows(IllegalArgumentException.class, () -> history.addAt(100, ""));
        assertEquals(Optional.of("100"), history.valueAtOrBefore(100));
        assertEquals(Optional.of("100"), history.valueAtOrBefore(201));

        history.addAt(200, "200");
        assertEquals(Optional.empty(), history.valueAtOrBefore(99));
        assertEquals(Optional.of("100"), history.valueAtOrBefore(100));
        assertEquals(Optional.of("100"), history.valueAtOrBefore(101));
        assertEquals(Optional.of("100"), history.valueAtOrBefore(199));
        assertEquals(Optional.of("200"), history.valueAtOrBefore(200));
        assertEquals(Optional.of("200"), history.valueAtOrBefore(201));
        assertEquals(Optional.of(new LogHistory.Entry<>(200, "200")), history.lastEntry());
    }

    @Test
    void testTruncateTo() {
        TreeMapLogHistory<String> history = new TreeMapLogHistory<>();
        history.addAt(100, "100");
        history.addAt(200, "200");

        history.truncateNewEntries(201);
        assertEquals(Optional.of(new LogHistory.Entry<>(200, "200")), history.lastEntry());

        history.truncateNewEntries(200);
        assertEquals(Optional.of(new LogHistory.Entry<>(100, "100")), history.lastEntry());

        history.truncateNewEntries(101);
        assertEquals(Optional.of(new LogHistory.Entry<>(100, "100")), history.lastEntry());

        history.truncateNewEntries(100);
        assertEquals(Optional.empty(), history.lastEntry());
    }

    @Test
    void testTrimPrefixTo() {
        TreeMapLogHistory<String> history = new TreeMapLogHistory<>();
        history.addAt(100, "100");
        history.addAt(200, "200");

        history.truncateOldEntries(99);
        assertEquals(Optional.empty(), history.valueAtOrBefore(99));
        assertEquals(Optional.of("100"), history.valueAtOrBefore(100));

        history.truncateOldEntries(100);
        assertEquals(Optional.empty(), history.valueAtOrBefore(99));
        assertEquals(Optional.of("100"), history.valueAtOrBefore(100));

        history.truncateOldEntries(101);
        assertEquals(Optional.empty(), history.valueAtOrBefore(99));
        assertEquals(Optional.of("100"), history.valueAtOrBefore(100));

        history.truncateOldEntries(200);
        assertEquals(Optional.empty(), history.valueAtOrBefore(199));
        assertEquals(Optional.of("200"), history.valueAtOrBefore(200));
    }

    @Test
    void testClear() {
        TreeMapLogHistory<String> history = new TreeMapLogHistory<>();
        history.addAt(100, "100");
        history.addAt(200, "200");
        history.clear();
        assertEquals(Optional.empty(), history.lastEntry());
    }
}
