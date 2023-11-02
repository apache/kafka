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
package org.apache.kafka.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

public class AbstractIteratorTest {

    @Test
    public void testIterator() {
        int max = 10;
        List<Integer> l = new ArrayList<>();
        for (int i = 0; i < max; i++)
            l.add(i);
        ListIterator<Integer> iter = new ListIterator<>(l);
        for (int i = 0; i < max; i++) {
            Integer value = i;
            assertEquals(value, iter.peek());
            assertTrue(iter.hasNext());
            assertEquals(value, iter.next());
        }
        assertFalse(iter.hasNext());
    }

    @Test
    public void testEmptyIterator() {
        Iterator<Object> iter = new ListIterator<>(Collections.emptyList());
        assertThrows(NoSuchElementException.class, iter::next);
    }

    static class ListIterator<T> extends AbstractIterator<T> {
        private List<T> list;
        private int position = 0;

        public ListIterator(List<T> l) {
            this.list = l;
        }

        public T makeNext() {
            if (position < list.size())
                return list.get(position++);
            else
                return allDone();
        }
    }
}
