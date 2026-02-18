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
package org.apache.kafka.common.test;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.function.Supplier;

import static java.lang.String.format;

/**
 * Helper functions for writing unit tests.
 * <p>
 * <b>Package-private:</b> Not intended for use outside {@code org.apache.kafka.common.test}.
 * Use {@code org/apache/kafka/test/TestUtils} instead.
 */
class TestUtils {
    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    /* A consistent random number generator to make tests repeatable */
    public static final Random SEEDED_RANDOM = new Random(192348092834L);
    
    public static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static final String DIGITS = "0123456789";
    public static final String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    private static final long DEFAULT_POLL_INTERVAL_MS = 100;
    private static final long DEFAULT_MAX_WAIT_MS = 15_000;

    /**
     * Create an empty file in the default temporary-file directory, using `kafka` as the prefix and `tmp` as the
     * suffix to generate its name.
     */
    public static File tempFile() throws IOException {
        final File file = Files.createTempFile("kafka", ".tmp").toFile();
        file.deleteOnExit();
        return file;
    }

    /**
     * Generate a random string of letters and digits of the given length
     *
     * @param len The length of the string
     * @return The random string
     */
    public static String randomString(final int len) {
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++)
            b.append(LETTERS_AND_DIGITS.charAt(SEEDED_RANDOM.nextInt(LETTERS_AND_DIGITS.length())));
        return b.toString();
    }

    /**
     * Create a temporary relative directory in the specified parent directory with the given prefix.
     *
     */
    static File tempDirectory() {
        final File file;
        String prefix = "kafka-";
        try {
            file = Files.createTempDirectory(prefix).toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }

        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException e) {
                log.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        });

        return file;
    }

    /**
     * uses default value of 15 seconds for timeout
     */
    public static void waitForCondition(final Supplier<Boolean> testCondition, final String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, () -> conditionDetails);
    }

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final Supplier<Boolean> testCondition,
                                        final long maxWaitMs,
                                        final Supplier<String> conditionDetails) throws InterruptedException {
        final long expectedEnd = System.currentTimeMillis() + maxWaitMs;

        while (true) {
            try {
                if (testCondition.get()) {
                    return;
                }
                String conditionDetail = conditionDetails.get() == null ? "" : conditionDetails.get();
                throw new TimeoutException("Condition not met: " + conditionDetail);
            } catch (final AssertionError t) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw t;
                }
            } catch (final Exception e) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw new AssertionError(format("Assertion failed with an exception after %s ms", maxWaitMs), e);
                }
            }
            Thread.sleep(Math.min(DEFAULT_POLL_INTERVAL_MS, maxWaitMs));
        }
    }

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final Supplier<Boolean> testCondition,
                                        final long maxWaitMs,
                                        String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, maxWaitMs, () -> conditionDetails);
    }
}
