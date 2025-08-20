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

/**
 * A filter that selectively includes tests that are not present in a given test catalog.
 * <p>
 * The format of the test catalog is a text file where each line has the format of:
 * <pre>
 *     FullyQualifiedClassName "#" MethodName "\n"
 * </pre>
 * If the catalog is missing, empty, or invalid, this filter will include all tests by default.
 */
public class CatalogTestFilter implements Filter<TestDescriptor> {

    private static final Filter<TestDescriptor> EXCLUDE_ALL_TESTS = testDescriptor -> FilterResult.excluded("missing catalog");

    private static final Logger log = LoggerFactory.getLogger(CatalogTestFilter.class);

    private final Set<TestAndMethod> testCatalog;

    CatalogTestFilter(Set<TestAndMethod> testCatalog) {
        this.testCatalog = Collections.unmodifiableSet(testCatalog);
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
        if (testCatalog.contains(testAndMethod)) {
            return FilterResult.excluded(null);
        } else {
            return FilterResult.included("new test");
        }
    }

    /**
     * Create a filter that excludes tests that are missing from a given test catalog file.
     * @param testCatalogFileName path to a test catalog file
     */
    public static Filter<TestDescriptor> create(String testCatalogFileName) {
        if (testCatalogFileName == null || testCatalogFileName.isEmpty()) {
            log.debug("No test catalog specified, will not select any tests with this filter.");
            return EXCLUDE_ALL_TESTS;
        }
        Path path = Paths.get(testCatalogFileName);
        log.debug("Loading test catalog file {}.", path);

        if (!Files.exists(path)) {
            log.error("Test catalog file {} does not exist, will not select any tests with this filter.", path);
            return EXCLUDE_ALL_TESTS;
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
            log.error("Error while reading test catalog file, will not select any tests with this filter.", e);
            return EXCLUDE_ALL_TESTS;
        }

        if (allTests.isEmpty()) {
            log.error("Loaded an empty test catalog, will not select any tests with this filter.");
            return EXCLUDE_ALL_TESTS;
        } else {
            log.debug("Loaded {} test methods from test catalog file {}.", allTests.size(), path);
            return new CatalogTestFilter(allTests);
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
