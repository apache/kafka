/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FilteredIteratorTest {

    @Test
    public void testFiltering() {
        List<Integer> list = Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5);

        Iterator<String> filtered = new FilteredIterator<String, Integer>(list.iterator()) {
            protected String filter(Integer i) {
                if (i % 3 == 0) return i.toString();
                return null;
            }
        };

        List<String> expected = Arrays.asList("3", "9", "6", "3");
        List<String> result = new ArrayList<String>();

        while (filtered.hasNext()) {
            result.add(filtered.next());
        }

        assertEquals(expected, result);
    }

    @Test
    public void testEmptySource() {
        List<Integer> list = new ArrayList<Integer>();

        Iterator<String> filtered = new FilteredIterator<String, Integer>(list.iterator()) {
            protected String filter(Integer i) {
                if (i % 3 == 0) return i.toString();
                return null;
            }
        };

        List<String> expected = new ArrayList<String>();
        List<String> result = new ArrayList<String>();

        while (filtered.hasNext()) {
            result.add(filtered.next());
        }

        assertEquals(expected, result);
    }

    @Test
    public void testNoMatch() {
        List<Integer> list = Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5);

        Iterator<String> filtered = new FilteredIterator<String, Integer>(list.iterator()) {
            protected String filter(Integer i) {
                if (i % 7 == 0) return i.toString();
                return null;
            }
        };

        List<String> expected = new ArrayList<String>();
        List<String> result = new ArrayList<String>();

        while (filtered.hasNext()) {
            result.add(filtered.next());
        }

        assertEquals(expected, result);
    }

}
