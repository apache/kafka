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

/**
 * A JUnit test filter which can include or exclude discovered tests before
 * they are sent off to the test engine for execution. The behavior of this
 * filter is controlled by the system property "kafka.test.run.quarantined".
 * If the property is set to "true", then only auto-quarantined and explicitly
 * {@code @Flaky} tests will be included. If the property is set to "false", then
 * only non-quarantined tests will be run.
 * <p>
 * This filter is registered with JUnit using SPI. The test-common-runtime module
 * includes a META-INF/services/org.junit.platform.launcher.PostDiscoveryFilter
 * service file which registers this class.
 */
public class QuarantinedPostDiscoveryFilter implements PostDiscoveryFilter {

    private static final TestTag FLAKY_TEST_TAG = TestTag.create("flaky");

    public static final String RUN_NEW_PROP = "kafka.test.run.new";

    public static final String RUN_FLAKY_PROP = "kafka.test.run.flaky";

    public static final String CATALOG_FILE_PROP = "kafka.test.catalog.file";

    private final Filter<TestDescriptor> autoQuarantinedFilter;

    private final boolean runNew;

    private final boolean runFlaky;

    // No-arg public constructor for SPI
    @SuppressWarnings("unused")
    public QuarantinedPostDiscoveryFilter() {
        runNew = System.getProperty(RUN_NEW_PROP, "false")
            .equalsIgnoreCase("true");

        runFlaky = System.getProperty(RUN_FLAKY_PROP, "false")
            .equalsIgnoreCase("true");

        String testCatalogFileName = System.getProperty(CATALOG_FILE_PROP);
        autoQuarantinedFilter = AutoQuarantinedTestFilter.create(testCatalogFileName, runNew);
    }

    // Visible for tests
    QuarantinedPostDiscoveryFilter(
        Filter<TestDescriptor> autoQuarantinedFilter,
        boolean runNew,
        boolean runFlaky
    ) {
        this.autoQuarantinedFilter = autoQuarantinedFilter;
        this.runNew = runNew;
        this.runFlaky = runFlaky;
    }

    @Override
    public FilterResult apply(TestDescriptor testDescriptor) {
        boolean hasFlakyTag = testDescriptor.getTags().contains(FLAKY_TEST_TAG);
        FilterResult result = autoQuarantinedFilter.apply(testDescriptor);

        if (runFlaky && runNew) {
            //  If selecting flaky and quarantined tests, we first check for explicitly flaky tests.
            //  If no flaky tag is set, defer to the auto-quarantined filter.
            if (hasFlakyTag) {
                return FilterResult.included("flaky");
            } else {
                return result;
            }
        } else if (runFlaky) {
            // If selecting only flaky, just check the tag. Don't use the auto-quarantined filter
            if (hasFlakyTag) {
                return FilterResult.included("flaky");
            } else {
                return FilterResult.excluded("non-flaky");
            }
        } else {
            // Running only auto-quarantined
            if (result.included() && hasFlakyTag) {
                return FilterResult.excluded("flaky");
            } else {
                return result;
            }
        }
    }
}
