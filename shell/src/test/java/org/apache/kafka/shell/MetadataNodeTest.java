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

package org.apache.kafka.shell;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.shell.MetadataNode.DirectoryNode;
import org.apache.kafka.shell.MetadataNode.FileNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

@Timeout(value = 120000, unit = MILLISECONDS)
public class MetadataNodeTest {
    @Test
    public void testMkdirs() {
        DirectoryNode root = new DirectoryNode();
        DirectoryNode defNode = root.mkdirs("abc", "def");
        DirectoryNode defNode2 = root.mkdirs("abc", "def");
        assertTrue(defNode == defNode2);
        DirectoryNode defNode3 = root.directory("abc", "def");
        assertTrue(defNode == defNode3);
        root.mkdirs("ghi");
        assertEquals(new HashSet<>(Arrays.asList("abc", "ghi")), root.children().keySet());
        assertEquals(Collections.singleton("def"), root.mkdirs("abc").children().keySet());
        assertEquals(Collections.emptySet(), defNode.children().keySet());
    }

    @Test
    public void testRmrf() {
        DirectoryNode root = new DirectoryNode();
        DirectoryNode foo = root.mkdirs("foo");
        foo.mkdirs("a");
        foo.mkdirs("b");
        root.mkdirs("baz");
        assertEquals(new HashSet<>(Arrays.asList("foo", "baz")), root.children().keySet());
        root.rmrf("foo", "a");
        assertEquals(new HashSet<>(Arrays.asList("b")), foo.children().keySet());
        root.rmrf("foo");
        assertEquals(new HashSet<>(Collections.singleton("baz")), root.children().keySet());
    }

    @Test
    public void testCreateFiles() {
        DirectoryNode root = new DirectoryNode();
        DirectoryNode abcdNode = root.mkdirs("abcd");
        FileNode quuxNodde = abcdNode.create("quux");
        quuxNodde.setContents("quux contents");
        assertEquals("quux contents", quuxNodde.contents());
        assertThrows(NotDirectoryException.class, () -> root.mkdirs("abcd", "quux"));
    }
}
