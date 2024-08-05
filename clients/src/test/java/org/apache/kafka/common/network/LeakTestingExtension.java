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
package org.apache.kafka.common.network;

import org.apache.kafka.common.utils.LeakTester;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.AssertionFailedError;

public abstract class LeakTestingExtension {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(LeakTestingExtension.class);
    private static final String ROOT_TESTER_INSTANCE = "leak-tester";
    private static final String PER_TEST_INSTANCE = "leak-test";

    protected abstract String message();

    private NetworkContextLeakTester tester(ExtensionContext extensionContext) {
        // Use the root namespace which lives across multiple test suites.
        ExtensionContext.Store store = extensionContext.getRoot().getStore(NAMESPACE);
        return store.getOrComputeIfAbsent(
                ROOT_TESTER_INSTANCE,
                ignored -> new NetworkContextLeakTester(),
                NetworkContextLeakTester.class);
    }

    protected void before(ExtensionContext extensionContext) {
        extensionContext.getStore(NAMESPACE).put(PER_TEST_INSTANCE, tester(extensionContext).start());
    }

    protected void after(ExtensionContext extensionContext) throws AssertionFailedError {
        try {
            ExtensionContext.Store store = extensionContext.getStore(NAMESPACE);
            store.getOrDefault(PER_TEST_INSTANCE, LeakTester.LeakTest.class, () -> {}).close();
        } catch (AssertionFailedError e) {
            throw new AssertionFailedError(message(), e);
        }
    }

    /**
     * This class applies a coarse leak test for a whole class at a time.
     */
    public static class All extends LeakTestingExtension implements BeforeAllCallback, AfterAllCallback {

        protected String message() {
            return "This test suite contains a resource leak. Close the resources.";
        }

        @Override
        public void beforeAll(ExtensionContext extensionContext) throws AssertionFailedError {
            before(extensionContext);
        }

        @Override
        public void afterAll(ExtensionContext extensionContext) throws AssertionFailedError {
            after(extensionContext);
        }
    }

    /**
     * This class applies a fine leak test for individual tests.
     */
    public static class Each extends LeakTestingExtension implements BeforeEachCallback, AfterEachCallback {

        protected String message() {
            return "This test method contains a resource leak. Close the resources.";
        }

        @Override
        public void beforeEach(ExtensionContext extensionContext) throws AssertionFailedError {
            before(extensionContext);
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) throws AssertionFailedError {
            after(extensionContext);
        }
    }
}
