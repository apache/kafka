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
package org.apache.kafka.streams.processor.internals;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class QuickUnionTest {

    @Test
    public void testUnite() {
        final QuickUnion<Long> qu = new QuickUnion<>();

        final long[] ids = {
            1L, 2L, 3L, 4L, 5L
        };

        for (final long id : ids) {
            qu.add(id);
        }

        assertEquals(5, roots(qu, ids).size());

        qu.unite(1L, 2L);
        assertEquals(4, roots(qu, ids).size());
        assertEquals(qu.root(1L), qu.root(2L));

        qu.unite(3L, 4L);
        assertEquals(3, roots(qu, ids).size());
        assertEquals(qu.root(1L), qu.root(2L));
        assertEquals(qu.root(3L), qu.root(4L));

        qu.unite(1L, 5L);
        assertEquals(2, roots(qu, ids).size());
        assertEquals(qu.root(1L), qu.root(2L));
        assertEquals(qu.root(2L), qu.root(5L));
        assertEquals(qu.root(3L), qu.root(4L));

        qu.unite(3L, 5L);
        assertEquals(1, roots(qu, ids).size());
        assertEquals(qu.root(1L), qu.root(2L));
        assertEquals(qu.root(2L), qu.root(3L));
        assertEquals(qu.root(3L), qu.root(4L));
        assertEquals(qu.root(4L), qu.root(5L));
    }

    @Test
    public void testUniteMany() {
        final QuickUnion<Long> qu = new QuickUnion<>();

        final long[] ids = {
            1L, 2L, 3L, 4L, 5L
        };

        for (final long id : ids) {
            qu.add(id);
        }

        assertEquals(5, roots(qu, ids).size());

        qu.unite(1L, 2L, 3L, 4L);
        assertEquals(2, roots(qu, ids).size());
        assertEquals(qu.root(1L), qu.root(2L));
        assertEquals(qu.root(2L), qu.root(3L));
        assertEquals(qu.root(3L), qu.root(4L));
        assertNotEquals(qu.root(1L), qu.root(5L));
    }

    private Set<Long> roots(final QuickUnion<Long> qu, final long... ids) {
        final HashSet<Long> roots = new HashSet<>();
        for (final long id : ids) {
            roots.add(qu.root(id));
        }
        return roots;
    }
}
