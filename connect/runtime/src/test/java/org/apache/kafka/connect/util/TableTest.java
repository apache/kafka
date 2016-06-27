/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.util;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TableTest {

    @Test
    public void basicOperations() {
        Table<String, Integer, String> table = new Table<>();
        table.put("foo", 5, "bar");
        table.put("foo", 6, "baz");
        assertEquals("bar", table.get("foo", 5));
        assertEquals("baz", table.get("foo", 6));

        Map<Integer, String> row = table.row("foo");
        assertEquals("bar", row.get(5));
        assertEquals("baz", row.get(6));

        assertEquals("bar", table.remove("foo", 5));
        assertNull(table.get("foo", 5));
        assertEquals("baz", table.remove("foo", 6));
        assertNull(table.get("foo", 6));
        assertTrue(table.row("foo").isEmpty());
    }

}
