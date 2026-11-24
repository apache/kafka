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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.apache.kafka.test.StreamsTestUtils.valuesToSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs the full {@link AbstractSessionBytesStoreTest} suite against the in-memory session store with
 * transactional state stores enabled, so the staged-write/merge path is held to the same behavioural
 * contract as the non-transactional store. Adds transactional-specific cases that the base suite (which
 * only ever reads staged, un-committed writes) does not cover.
 */
public class InMemoryTransactionalSessionStoreTest extends AbstractSessionBytesStoreTest {

    @Override
    StoreType storeType() {
        return StoreType.InMemoryStore;
    }

    @Override
    boolean transactional() {
        return true;
    }

    @Test
    public void shouldReadAcrossCommitBoundaries() {
        final Windowed<String> a0 = new Windowed<>("a", new SessionWindow(0, 0));
        final Windowed<String> a1 = new Windowed<>("a", new SessionWindow(10, 20));

        sessionStore.put(a0, 1L);                         // staged
        sessionStore.commit(Collections.emptyMap());      // -> committed
        sessionStore.put(a1, 2L);                         // staged on top of committed

        // committed (a0) and staged (a1) are both visible before commit
        try (final KeyValueIterator<Windowed<String>, Long> it = sessionStore.fetch("a")) {
            assertEquals(Set.of(1L, 2L), valuesToSet(it));
        }
        sessionStore.commit(Collections.emptyMap());      // a1 -> committed; still visible
        try (final KeyValueIterator<Windowed<String>, Long> it = sessionStore.fetch("a")) {
            assertEquals(Set.of(1L, 2L), valuesToSet(it));
        }

        // a staged tombstone hides the committed session, and stays hidden after commit
        sessionStore.remove(a0);
        try (final KeyValueIterator<Windowed<String>, Long> it = sessionStore.fetch("a")) {
            assertEquals(Set.of(2L), valuesToSet(it));
        }
        sessionStore.commit(Collections.emptyMap());
        try (final KeyValueIterator<Windowed<String>, Long> it = sessionStore.fetch("a")) {
            assertEquals(Set.of(2L), valuesToSet(it));
        }
    }
}
