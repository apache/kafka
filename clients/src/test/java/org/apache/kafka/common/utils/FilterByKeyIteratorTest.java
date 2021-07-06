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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FilterByKeyIteratorTest {

    @Test
    public void testFilterByKey() {
        List<Header> headers = Arrays.asList(new RecordHeader("a", "b".getBytes()),
            new RecordHeader("b", "b".getBytes()),
            new RecordHeader("c", "b".getBytes()));

        Iterator<Header> iter = new FilterByKeyIterator<>(headers.iterator(), h -> h.key().equals("c"));
        assertTrue(iter.hasNext());
        assertEquals(new RecordHeader("c", "b".getBytes()), iter.next());
        assertFalse(iter.hasNext());
    }
}

