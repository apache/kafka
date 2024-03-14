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
package org.apache.kafka.common.utils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Interface for leak-checkers which evaluates an execution interval and throws an exception if resources were leaked.
 * <p>A test interval is the time between {@link LeakTester#start()} and {@link LeakTest#close()}. A leak-checker
 * should consider a resource leaked if it is opened during the interval, but not closed during that interval.
 * Implementations of this interface should be thread-safe.
 *
 * <p>For example, the following situation would :
 * <pre>
 * {@code
 * LeakTester tester = ; // some implementation
 * AutoCloseable resource;
 * try (LeakTest test : tester.start()) {
 *     resource = () -> {};
 *     tester.register(resource); // pseudocode
 *     resource.close(); // If this is missing, a leak will be detected.
 * } catch (AssertionError e) {
 *     // We leaked a resource!
 * }
 * } </pre>
 *
 * <p>Implementations of LeakTester will have different ways of registering resources for tracking. For example,
 * A LeakTester that is also a Factory could automatically register all resources created by the factory method(s).
 * A LeakTester could also maintain static methods which allows registration from deeper in the call stack.
 * Implementations of LeakTester will also have different ways of testing if resources are closed, depending on the
 * specifics of the resource they are tracking.
 */
@FunctionalInterface
public interface LeakTester {

    /**
     * Start a leak testing interval
     * @return A {@link LeakTest} object which when closed, defines the end of the interval and checks for leaks.
     */
    LeakTest start();

    /**
     * A leak test that has been started
     */
    @FunctionalInterface
    interface LeakTest extends AutoCloseable {
        /**
         * Stop the leak test
         * @throws AssertionError if a resource was opened during the interval, but not closed.
         */
        void close() throws AssertionError;
    }

    /**
     * Combine two or more LeakTester objects into a single test. This has the effect of running multiple LeakTesters
     * concurrently. If one or more of the testers discovers a leak, their exceptions are suppressed and a new exception
     * is thrown to summarize all failures.
     * @param testers A group of LeakTester instances which should be run concurrently
     * @return A combined leak test which runs tests for the passed-in testers concurrently, non-null.
     */
    static LeakTester combine(LeakTester... testers) {
        return () -> {
            List<LeakTest> tests = Arrays.stream(testers).map(LeakTester::start).collect(Collectors.toList());
            return () -> {
                AssertionError summary = null;
                for (LeakTest test : tests) {
                    try {
                        test.close();
                    } catch (AssertionError e) {
                        if (summary == null) {
                            summary = new AssertionError("Leak check failed");
                        }
                        summary.addSuppressed(e);
                    }
                }
                if (summary != null) {
                    throw summary;
                }
            };
        };
    }
}
