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
import org.junit.platform.engine.TestTag;
import org.junit.platform.launcher.PostDiscoveryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit test filter that customizes which tests are run as part of different CI jobs.
 * <p>
 * Four system properties control the behavior of this filter:
 * <ul>
 *   <li>kafka.test.run.new: only run newly added tests</li>
 *   <li>kafka.test.flaky.new: only run tests tagged with "flaky"</li>
 *   <li>kafka.test.catalog.file: location of a test catalog file</li>
 *   <li>kafka.test.verbose: enable additional log output</li>
 * </ul>
 * The test catalog is how we test for "new"-ness. During each CI build of "trunk", we
 * produce a test catalog that includes every test run as part of the build. This is
 * stored in the "test-catalog" branch of the repo. By loading the test catalog from a
 * prior point in time, we can easily determine which tests were added within a certain
 * time frame.
 * <p>
 * If no test catalog is given, or it is empty/invalid, "kafka.test.run.new" will not
 * select any tests.
 * <p>
 * This filter is registered with JUnit using SPI. The test-common-runtime module
 * includes a META-INF/services/org.junit.platform.launcher.PostDiscoveryFilter
 * service file which registers this class.
 */
public class KafkaPostDiscoveryFilter implements PostDiscoveryFilter {

    private static final TestTag FLAKY_TEST_TAG = TestTag.create("flaky");

    public static final String RUN_NEW_PROP = "kafka.test.run.new";

    public static final String RUN_FLAKY_PROP = "kafka.test.run.flaky";

    public static final String CATALOG_FILE_PROP = "kafka.test.catalog.file";

    public static final String VERBOSE_PROP = "kafka.test.verbose";

    private static final Logger log = LoggerFactory.getLogger(KafkaPostDiscoveryFilter.class);

    private final Filter<TestDescriptor> catalogFilter;

    private final boolean runNew;

    private final boolean runFlaky;

    private final boolean verbose;

    // No-arg public constructor for SPI
    @SuppressWarnings("unused")
    public KafkaPostDiscoveryFilter() {
        runNew = System.getProperty(RUN_NEW_PROP, "false")
            .equalsIgnoreCase("true");

        runFlaky = System.getProperty(RUN_FLAKY_PROP, "false")
            .equalsIgnoreCase("true");

        verbose = System.getProperty(VERBOSE_PROP, "false")
            .equalsIgnoreCase("true");

        String testCatalogFileName = System.getProperty(CATALOG_FILE_PROP);
        catalogFilter = CatalogTestFilter.create(testCatalogFileName);
    }

    // Visible for tests
    KafkaPostDiscoveryFilter(
        Filter<TestDescriptor> catalogFilter,
        boolean runNew,
        boolean runFlaky
    ) {
        this.catalogFilter = catalogFilter;
        this.runNew = runNew;
        this.runFlaky = runFlaky;
        this.verbose = false;
    }

    @Override
    public FilterResult apply(TestDescriptor testDescriptor) {
        boolean hasFlakyTag = testDescriptor.getTags().contains(FLAKY_TEST_TAG);
        FilterResult catalogFilterResult = catalogFilter.apply(testDescriptor);

        final FilterResult result;
        if (runFlaky && runNew) {
            //  If selecting flaky and new tests, we first check for explicitly flaky tests.
            //  If no flaky tag is set, defer to the catalog filter.
            if (hasFlakyTag) {
                result = FilterResult.included("flaky");
            } else {
                result = catalogFilterResult;
            }
        } else if (runFlaky) {
            // If selecting only flaky, just check the tag. Don't use the catalog filter
            if (hasFlakyTag) {
                result = FilterResult.included("flaky");
            } else {
                result = FilterResult.excluded("non-flaky");
            }
        } else if (runNew) {
            // Running only new tests (per the catalog filter)
            if (catalogFilterResult.included() && hasFlakyTag) {
                result = FilterResult.excluded("flaky");
            } else {
                result = catalogFilterResult;
            }
        } else {
            // The main test suite
            if (hasFlakyTag) {
                result = FilterResult.excluded("flaky");
            } else if (catalogFilterResult.included()) {
                result = FilterResult.excluded("new");
            } else {
                result = FilterResult.included(null);
            }
        }

        if (verbose) {
            log.info(
                "{} Test '{}' with reason '{}'. Flaky tag is {}, catalog filter has {} this test.",
                result.included() ? "Including" : "Excluding",
                testDescriptor.getDisplayName(),
                result.getReason().orElse("null"),
                hasFlakyTag ? "present" : "not present",
                catalogFilterResult.included() ? "included" : "not included"
            );
        }
        return result;
    }
}
