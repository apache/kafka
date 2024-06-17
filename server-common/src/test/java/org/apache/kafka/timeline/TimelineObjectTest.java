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

package org.apache.kafka.timeline;

import java.util.Collections;

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class TimelineObjectTest {
    @Test
    public void testModifyValue() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineObject<String> object = new TimelineObject<>(registry, "default");
        assertEquals("default", object.get());
        assertEquals("default", object.get(Long.MAX_VALUE));
        object.set("1");
        object.set("2");
        assertEquals("2", object.get());
        assertEquals("2", object.get(Long.MAX_VALUE));
    }

    @Test
    public void testToStringAndEquals() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineObject<String> object = new TimelineObject<>(registry, "");
        assertEquals("", object.toString());
        object.set("a");
        TimelineObject<String> object2 = new TimelineObject<>(registry, "");
        object2.set("a");
        assertEquals("a", object2.toString());
        assertEquals(object, object2);
    }

    @Test
    public void testSnapshot() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineObject<String> object = new TimelineObject<>(registry, "1000");
        registry.getOrCreateSnapshot(2);
        object.set("1001");
        registry.getOrCreateSnapshot(3);
        object.set("1002");
        object.set("1003");
        object.set("1002");
        registry.getOrCreateSnapshot(4);
        assertEquals("1000", object.get(2));
        assertEquals("1001", object.get(3));
        assertEquals("1002", object.get(4));
        registry.revertToSnapshot(3);
        assertEquals("1001", object.get());
        registry.revertToSnapshot(2);
        assertEquals("1000", object.get());
    }

    @Test
    public void testReset() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineObject<String> value = new TimelineObject<>(registry, "<default>");
        registry.getOrCreateSnapshot(2);
        value.set("first value");
        registry.getOrCreateSnapshot(3);
        value.set("second value");

        registry.reset();

        assertEquals(Collections.emptyList(), registry.epochsList());
        assertEquals("<default>", value.get());
    }
}
