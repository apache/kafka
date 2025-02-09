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

import org.junit.platform.engine.Filter;
import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class AutoQuarantinedTestFilter implements Filter<TestDescriptor> {

    private static final Filter<TestDescriptor> INCLUDE_ALL_TESTS = testDescriptor -> FilterResult.included(null);
    private static final Filter<TestDescriptor> EXCLUDE_ALL_TESTS = testDescriptor -> FilterResult.excluded(null);

    private static final Logger log = LoggerFactory.getLogger(AutoQuarantinedTestFilter.class);

    private final Set<TestAndMethod> testCatalog;
    private final boolean includeQuarantined;

    AutoQuarantinedTestFilter(Set<TestAndMethod> testCatalog, boolean includeQuarantined) {
        this.testCatalog = Collections.unmodifiableSet(testCatalog);
        this.includeQuarantined = includeQuarantined;
    }

    @Override
    public FilterResult apply(TestDescriptor testDescriptor) {
        Optional<TestSource> sourceOpt = testDescriptor.getSource();
        if (sourceOpt.isEmpty()) {
            return FilterResult.included(null);
        }

        TestSource source = sourceOpt.get();
        if (!(source instanceof MethodSource)) {
            return FilterResult.included(null);
        }

        MethodSource methodSource = (MethodSource) source;

        TestAndMethod testAndMethod = new TestAndMethod(methodSource.getClassName(), methodSource.getMethodName());
        if (includeQuarantined) {
            if (testCatalog.contains(testAndMethod)) {
                return FilterResult.excluded("exclude non-quarantined");
            } else {
                return FilterResult.included("auto-quarantined");
            }
        } else {
            if (testCatalog.contains(testAndMethod)) {
                return FilterResult.included(null);
            } else {
                return FilterResult.excluded("auto-quarantined");
            }
        }
    }

    private static Filter<TestDescriptor> defaultFilter(boolean includeQuarantined) {
        if (includeQuarantined) {
            return EXCLUDE_ALL_TESTS;
        } else {
            return INCLUDE_ALL_TESTS;
        }
    }

    /**
     * Create a filter that excludes tests that are missing from a given test catalog file.
     * If no test catalog is given, the default behavior depends on {@code includeQuarantined}.
     * If true, this filter will exclude all tests. If false, this filter will include all tests.
     * <p>
     * The format of the test catalog is a text file where each line has the format of:
     *
     * <pre>
     *     FullyQualifiedClassName "#" MethodName "\n"
     * </pre>
     *
     * @param testCatalogFileName path to a test catalog file
     * @param includeQuarantined true if this filter should include only the auto-quarantined tests
     */
    public static Filter<TestDescriptor> create(String testCatalogFileName, boolean includeQuarantined) {
        if (testCatalogFileName == null || testCatalogFileName.isEmpty()) {
            log.debug("No test catalog specified, will not quarantine any recently added tests.");
            return defaultFilter(includeQuarantined);
        }
        Path path = Paths.get(testCatalogFileName);
        log.debug("Loading test catalog file {}.", path);

        if (!Files.exists(path)) {
            log.error("Test catalog file {} does not exist, will not quarantine any recently added tests.", path);
            return defaultFilter(includeQuarantined);
        }

        Set<TestAndMethod> allTests = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(path, Charset.defaultCharset())) {
            String line = reader.readLine();
            while (line != null) {
                String[] toks = line.split("#", 2);
                allTests.add(new TestAndMethod(toks[0], toks[1]));
                line = reader.readLine();
            }
        } catch (IOException e) {
            log.error("Error while reading test catalog file, will not quarantine any recently added tests.", e);
            return defaultFilter(includeQuarantined);
        }

        if (allTests.isEmpty()) {
            log.error("Loaded an empty test catalog, will not quarantine any recently added tests.");
            return defaultFilter(includeQuarantined);
        } else {
            log.debug("Loaded {} test methods from test catalog file {}.", allTests.size(), path);
            return new AutoQuarantinedTestFilter(allTests, includeQuarantined);
        }
    }

    public static class TestAndMethod {
        private final String testClass;
        private final String testMethod;

        public TestAndMethod(String testClass, String testMethod) {
            this.testClass = testClass;
            this.testMethod = testMethod;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestAndMethod that = (TestAndMethod) o;
            return Objects.equals(testClass, that.testClass) && Objects.equals(testMethod, that.testMethod);
        }

        @Override
        public int hashCode() {
            return Objects.hash(testClass, testMethod);
        }

        @Override
        public String toString() {
            return "TestAndMethod{" +
                "testClass='" + testClass + '\'' +
                ", testMethod='" + testMethod + '\'' +
                '}';
        }
    }
}
