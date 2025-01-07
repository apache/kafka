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

package org.apache.kafka.common.test.junit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.Filter;
import org.junit.platform.engine.TestDescriptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AutoQuarantinedTestFilterTest {

    private TestDescriptor descriptor(String className, String methodName) {
        return new QuarantinedPostDiscoveryFilterTest.MockTestDescriptor(className, methodName);
    }

    @Test
    public void testLoadCatalog(@TempDir Path tempDir) throws IOException {
        Path catalog = tempDir.resolve("catalog.txt");
        List<String> lines = new ArrayList<>();
        lines.add("o.a.k.Foo#testBar1");
        lines.add("o.a.k.Foo#testBar2");
        lines.add("o.a.k.Spam#testEggs");
        Files.write(catalog, lines);

        Filter<TestDescriptor> filter = AutoQuarantinedTestFilter.create(catalog.toString(), false);
        assertTrue(filter.apply(descriptor("o.a.k.Foo", "testBar1")).included());
        assertTrue(filter.apply(descriptor("o.a.k.Foo", "testBar2")).included());
        assertTrue(filter.apply(descriptor("o.a.k.Spam", "testEggs")).included());
        assertTrue(filter.apply(descriptor("o.a.k.Spam", "testNew")).excluded());

        filter = AutoQuarantinedTestFilter.create(catalog.toString(), true);
        assertTrue(filter.apply(descriptor("o.a.k.Foo", "testBar1")).excluded());
        assertTrue(filter.apply(descriptor("o.a.k.Foo", "testBar2")).excluded());
        assertTrue(filter.apply(descriptor("o.a.k.Spam", "testEggs")).excluded());
        assertTrue(filter.apply(descriptor("o.a.k.Spam", "testNew")).included());
    }

    @Test
    public void testEmptyCatalog(@TempDir Path tempDir) throws IOException {
        Path catalog = tempDir.resolve("catalog.txt");
        Files.write(catalog, Collections.emptyList());

        Filter<TestDescriptor> filter = AutoQuarantinedTestFilter.create(catalog.toString(), false);
        assertTrue(filter.apply(descriptor("o.a.k.Foo", "testBar1")).included());
        assertTrue(filter.apply(descriptor("o.a.k.Foo", "testBar2")).included());
        assertTrue(filter.apply(descriptor("o.a.k.Spam", "testEggs")).included());
        assertTrue(filter.apply(descriptor("o.a.k.Spam", "testNew")).included());
    }

    @Test
    public void testMissingCatalog() {
        Filter<TestDescriptor> filter = AutoQuarantinedTestFilter.create("does-not-exist.txt", false);
        assertTrue(filter.apply(descriptor("o.a.k.Foo", "testBar1")).included());
        assertTrue(filter.apply(descriptor("o.a.k.Foo", "testBar2")).included());
        assertTrue(filter.apply(descriptor("o.a.k.Spam", "testEggs")).included());
        assertTrue(filter.apply(descriptor("o.a.k.Spam", "testNew")).included());
    }
}
