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
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.TestTag;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.MethodSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuarantinedPostDiscoveryFilterTest {

    static class MockTestDescriptor implements TestDescriptor {

        private final MethodSource methodSource;
        private final Set<TestTag> testTags;

        MockTestDescriptor(String className, String methodName, String... tags) {
            this.methodSource = MethodSource.from(className, methodName);
            this.testTags = new HashSet<>();
            Arrays.stream(tags).forEach(tag -> testTags.add(TestTag.create(tag)));
        }

        @Override
        public UniqueId getUniqueId() {
            return null;
        }

        @Override
        public String getDisplayName() {
            return "";
        }

        @Override
        public Set<TestTag> getTags() {
            return this.testTags;
        }

        @Override
        public Optional<TestSource> getSource() {
            return Optional.of(this.methodSource);
        }

        @Override
        public Optional<TestDescriptor> getParent() {
            return Optional.empty();
        }

        @Override
        public void setParent(TestDescriptor testDescriptor) {

        }

        @Override
        public Set<? extends TestDescriptor> getChildren() {
            return Set.of();
        }

        @Override
        public void addChild(TestDescriptor testDescriptor) {

        }

        @Override
        public void removeChild(TestDescriptor testDescriptor) {

        }

        @Override
        public void removeFromHierarchy() {

        }

        @Override
        public Type getType() {
            return null;
        }

        @Override
        public Optional<? extends TestDescriptor> findByUniqueId(UniqueId uniqueId) {
            return Optional.empty();
        }
    }

    QuarantinedPostDiscoveryFilter setupFilter(boolean runNew, boolean runFlaky) {
        Set<AutoQuarantinedTestFilter.TestAndMethod> testCatalog = new HashSet<>();
        testCatalog.add(new AutoQuarantinedTestFilter.TestAndMethod("o.a.k.Foo", "testBar1"));
        testCatalog.add(new AutoQuarantinedTestFilter.TestAndMethod("o.a.k.Foo", "testBar2"));
        testCatalog.add(new AutoQuarantinedTestFilter.TestAndMethod("o.a.k.Spam", "testEggs"));

        AutoQuarantinedTestFilter autoQuarantinedTestFilter = new AutoQuarantinedTestFilter(testCatalog);
        return new QuarantinedPostDiscoveryFilter(autoQuarantinedTestFilter, runNew, runFlaky);
    }

    @Test
    public void testExcludeExistingNonFlaky() {
        QuarantinedPostDiscoveryFilter filter = setupFilter(false, true);
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar1")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar2")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggs")).excluded());
    }

    @Test
    public void testIncludeExistingFlaky() {
        QuarantinedPostDiscoveryFilter filter = setupFilter(false, true);
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar1", "flaky")).included());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar2", "flaky")).included());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggs", "flaky", "integration")).included());
    }

    @Test
    public void testIncludeAutoQuarantinedAndFlaky() {
        QuarantinedPostDiscoveryFilter filter = setupFilter(true, true);
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar3")).included());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggz", "flaky")).included());
    }

    @Test
    public void testIncludeAutoQuarantinedNoFlaky() {
        QuarantinedPostDiscoveryFilter filter = setupFilter(true, false);
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar3")).included());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggz", "flaky")).excluded());
    }

    @Test
    public void testExcludeFlakyAndNew() {
        QuarantinedPostDiscoveryFilter filter = setupFilter(false, false);
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar3")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggz", "flaky")).excluded());
    }

    @Test
    public void testExcludeFlaky() {
        QuarantinedPostDiscoveryFilter filter = setupFilter(false, false);
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar1", "flaky")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar2", "flaky")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggs", "flaky", "integration")).excluded());
    }

    @Test
    public void testExistingTestNonFlaky() {
        QuarantinedPostDiscoveryFilter filter = setupFilter(false, false);
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar1")).included());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar2")).included());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggs")).included());
    }

    @Test
    public void testNoCatalogRunFlakyTests() {
        QuarantinedPostDiscoveryFilter filter = new QuarantinedPostDiscoveryFilter(
            AutoQuarantinedTestFilter.create(null),
            false, true
        );
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar1", "flaky")).included());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar2", "flaky")).included());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggs")).excluded());
    }

    @Test
    public void testNoCatalogRunNewTest() {
        QuarantinedPostDiscoveryFilter filter = new QuarantinedPostDiscoveryFilter(
                AutoQuarantinedTestFilter.create(null),
                true, false
        );
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar1", "flaky")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar2", "flaky")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggs")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testNew")).excluded(),
            "Should not select a new test because there is no catalog loaded");
    }

    @Test
    public void testNoCatalogRunMainTests() {
        QuarantinedPostDiscoveryFilter filter = new QuarantinedPostDiscoveryFilter(
                AutoQuarantinedTestFilter.create(null),
                false, false
        );
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar1", "flaky")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Foo", "testBar2", "flaky")).excluded());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testEggs")).included());
        assertTrue(filter.apply(new MockTestDescriptor("o.a.k.Spam", "testNew")).included());
    }
}
