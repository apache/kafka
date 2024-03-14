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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class LeakTestingExtension {

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.TYPE})
    public @interface Ignore {
    }

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(LeakTestingExtension.class);
    private static final String ROOT_TESTER_INSTANCE = "leak-tester";
    private static final String PER_TEST_INSTANCE = "leak-test";
    private static boolean ignored(ExtensionContext extensionContext) {
        boolean classHasIgnoreAnnotation = extensionContext.getTestClass()
                .map(clazz -> clazz.getAnnotationsByType(Ignore.class))
                .map(arr -> arr.length > 0)
                .orElse(false);
        boolean methodHasIgnoreAnnotation = extensionContext.getTestMethod()
                .map(method -> method.getAnnotationsByType(Ignore.class))
                .map(arr -> arr.length > 0)
                .orElse(false);
        return classHasIgnoreAnnotation || methodHasIgnoreAnnotation;
    }

    private static NetworkContextLeakTester tester(ExtensionContext extensionContext) {
        // Use the root namespace which lives across multiple test suites.
        ExtensionContext.Store store = extensionContext.getRoot().getStore(NAMESPACE);
        Object storeValue = store.getOrComputeIfAbsent(ROOT_TESTER_INSTANCE, ignored -> new NetworkContextLeakTester());
        NetworkContextLeakTester tester;
        if (storeValue instanceof NetworkContextLeakTester) {
            tester = (NetworkContextLeakTester) storeValue;
        } else {
            throw new RuntimeException("Unexpected value " + storeValue + " in context");
        }
        return tester;
    }

    private static void before(ExtensionContext extensionContext) {
        if (ignored(extensionContext)) {
            return;
        }
        extensionContext.getStore(NAMESPACE).put(PER_TEST_INSTANCE, tester(extensionContext).start());
    }

    private static void after(ExtensionContext extensionContext) {
        if (ignored(extensionContext)) {
            return;
        }
        Object o = extensionContext.getStore(NAMESPACE).get(PER_TEST_INSTANCE);
        if (o instanceof LeakTester.LeakTest) {
            try {
                ((LeakTester.LeakTest) o).close();
            } catch (AssertionError e) {
                throw new AssertionError("This test contains a resource leak. Close the resources instantiated in the test, or add the @LeakTestingExtension.Ignore annotation to the method and/or class.", e);
            }
        }
    }

    /**
     * This class applies a coarse leak test for a whole class at a time.
     * This is automatically loaded for all classes, but can be disabled with {@link Ignore}
     * See {@link org.junit.jupiter.api.extension.Extension} ServiceLoader manifest.
     */
    public static class All implements BeforeAllCallback, AfterAllCallback {

        @Override
        public void beforeAll(ExtensionContext extensionContext) throws Exception {
            before(extensionContext);
        }

        @Override
        public void afterAll(ExtensionContext extensionContext) throws Exception {
            after(extensionContext);
        }
    }

    /**
     * This class applies a fine leak test for individual tests.
     * This is automatically loaded for all classes, but can be disabled with {@link Ignore}
     * See {@link org.junit.jupiter.api.extension.Extension} ServiceLoader manifest.
     */
    public static class Each implements BeforeEachCallback, AfterEachCallback {

        @Override
        public void beforeEach(ExtensionContext extensionContext) throws Exception {
            if (ignored(extensionContext)) {
                return;
            }
            before(extensionContext);
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) throws Exception {
            if (ignored(extensionContext)) {
                return;
            }
            after(extensionContext);
        }
    }
}
