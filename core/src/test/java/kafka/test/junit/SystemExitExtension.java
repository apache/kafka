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

package kafka.test.junit;

import org.apache.kafka.common.utils.Exit;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SystemExitExtension implements BeforeEachCallback, AfterEachCallback {

    private static final Logger log = LoggerFactory.getLogger(SystemExitExtension.class);
    private static final Map<Object, TestCase> ACTIVE_TEST_CASES = new ConcurrentHashMap<>();
    private static final Map<Object, TestCase> FINISHED_TEST_CASES = new ConcurrentHashMap<>();
    private static final InheritableThreadLocal<Object> CURRENT_TEST_INSTANCE = new InheritableThreadLocal<>();

    public SystemExitExtension() {
        log.debug("Instantiating system exit extension to guard against unintentional calls to terminate the JVM");
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        addActiveTestCase(context);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        finishActiveTestCase(context);
        checkForLateExits();
    }

    /**
     * For testing the extension; should not be used by any other tests
     */
    static void allowExitFromCurrentTest() {
        Object testInstance = CURRENT_TEST_INSTANCE.get();
        assertNotNull(testInstance);
        TestCase testCase = ACTIVE_TEST_CASES.get(testInstance);
        assertNotNull(testCase);
        testCase.allowExit(true);
    }

    /**
     * For testing the extension; should not be used by any other tests
     */
    static void assertExitCalledFromCurrentTest(int expectedStatus) {
        Object testInstance = CURRENT_TEST_INSTANCE.get();
        assertNotNull(testInstance);
        TestCase testCase = ACTIVE_TEST_CASES.remove(testInstance);
        assertExitFromTest(testCase, expectedStatus);
    }

    /**
     * For testing the extension; should not be used by any other tests
     */
    static void assertExitFromPriorTest(Object testInstance, int expectedStatus) {
        TestCase testCase = FINISHED_TEST_CASES.remove(testInstance);
        assertExitFromTest(testCase, expectedStatus);
    }

    private static void assertExitFromTest(TestCase testCase, int expectedStatus) {
        assertNotNull(testCase);
        Integer actualStatus = testCase.exit();
        assertEquals(Integer.valueOf(expectedStatus), actualStatus);
    }

    private static void addActiveTestCase(ExtensionContext context) {
        Object testInstance = context.getRequiredTestInstance();
        CURRENT_TEST_INSTANCE.set(testInstance);
        Method testMethod = context.getRequiredTestMethod();
        String testCaseDescription = testMethod.getDeclaringClass() + "::" + testMethod.getName();
        ACTIVE_TEST_CASES.put(testInstance, new TestCase(testCaseDescription));
        Exit.setFallbackExitProcedure(exitHandler(testInstance));
        Exit.setFallbackHaltProcedure(exitHandler(testInstance));
    }

    private static void finishActiveTestCase(ExtensionContext context) {
        Exit.resetExitProcedure();
        Exit.resetHaltProcedure();
        // Any leaked threads from the test case will continue to see the value that they were instantiated with,
        // but this thread should be reset since it may be reused for newer tests
        CURRENT_TEST_INSTANCE.remove();

        Object testInstance = context.getRequiredTestInstance();
        synchronized (SystemExitExtension.class) {
            TestCase testCase = ACTIVE_TEST_CASES.remove(testInstance);
            if (testCase == null) {
                // Should never happen, but just in case...
                return;
            }
            if (testCase.exit() != null && !testCase.allowExit()) {
                throw new AssertionError("Exit/halt was invoked with status " + testCase.exit() + " during this test. "
                        + "This test should either be modified to install a custom exit/halt procedure in the Exit class, or "
                        + "if no calls to Exit::exit or Exit::halt are expected, fixed to prevent these calls from taking place.");
            } else {
                // Continue tracking tests that have completed without exiting as they may leak threads
                // that try to exit later on
                FINISHED_TEST_CASES.put(testInstance, testCase);
            }
        }
    }

    private static void checkForLateExits() {
        Collection<TestCase> lateExits;
        synchronized (SystemExitExtension.class) {
            lateExits = FINISHED_TEST_CASES.values().stream()
                    .filter(tc -> tc.exit() != null)
                    .filter(tc -> !tc.allowExit())
                    .collect(Collectors.toSet());
            FINISHED_TEST_CASES.values().removeAll(lateExits);
        }
        if (!lateExits.isEmpty()) {
            throw new AssertionError(
                    "Exit/halt was invoked by threads spawned for testing after those tests had completed; "
                    + "this test will fail in order to surface these illegal calls. The calls occurred in the following tests:\n"
                    + lateExits.stream().map(Object::toString).collect(Collectors.joining("\n"))
                    + "Since the attempts to terminate the JVM originated from potentially-leaked threads, it may not be sufficient to "
                    + "use the Exit wrapper class, since its behavior must be reset at the end of each test, at which point other threads "
                    + "spawned during testing may attempt to use it. The offending test may have to be modified or disabled."
            );
        }
    }

    private static class TestCase {

        private volatile boolean allowExit;
        private final String description;
        private final AtomicReference<Integer> exit;

        public TestCase(String description) {
            this.allowExit = false;
            this.description = Objects.requireNonNull(description);
            this.exit = new AtomicReference<>();
        }

        public void recordExit(int status) {
            exit.compareAndSet(null, status);
        }

        public Integer exit() {
            return exit.get();
        }

        // For testing only {
        public boolean allowExit() {
            return allowExit;
        }

        // For testing only
        public void allowExit(boolean allowExit) {
            this.allowExit = allowExit;
        }

        @Override
        public String toString() {
            String result = description;
            if (exit() != null) {
                result += " (exited with status " + exit() + ")";
            }
            return result;
        }
    }

    private static org.apache.kafka.common.utils.Exit.Procedure exitHandler(Object testInstance) {
        return (statusCode, message) -> {
            TestCase testCase;
            synchronized (SystemExitExtension.class) {
                testCase = ACTIVE_TEST_CASES.get(testInstance);
                if (testCase == null) {
                    testCase = FINISHED_TEST_CASES.get(testInstance);
                }
                if (testCase == null) {
                    // In this case, it's possible that a single test has caused multiple attempts to terminate the JVM
                    // to take place, and we've already removed it from our collection of both active and finished test
                    // cases since we only need to report at most one attempt per test
                    return;
                }

                testCase.recordExit(statusCode);
            }

            String errorMessage = "Exit or halt was invoked during test with status " + statusCode;
            if (message != null) {
                errorMessage += " and message '" + message + "'";
            }

            throw new AssertionError(errorMessage);
        };
    }

}
