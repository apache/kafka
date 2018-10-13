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
package org.apache.kafka.streams.internals;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public final class ApiUtils {
    private ApiUtils() {
    }

    /**
     * Validates that milliseconds from {@code duration} can be retrieved.
     * @param duration Duration to check.
     * @param name Name of params for an error message.
     * @return Milliseconds from {@code duration}.
     */
    public static long validateMillisecondDuration(final Duration duration, final String name) {
        try {
            if (duration == null)
                throw new IllegalArgumentException("[" + Objects.toString(name) + "] shouldn't be null.");

            return duration.toMillis();
        } catch (final ArithmeticException e) {
            throw new IllegalArgumentException("[" + name + "] can't be converted to milliseconds. ", e);
        }
    }

    /**
     * Validates that milliseconds from {@code instant} can be retrieved.
     * @param instant Instant to check.
     * @param name Name of params for an error message.
     * @return Milliseconds from {@code instant}.
     */
    public static long validateMillisecondInstant(final Instant instant, final String name) {
        try {
            if (instant == null)
                throw new IllegalArgumentException("[" + name + "] shouldn't be null.");

            return instant.toEpochMilli();
        } catch (final ArithmeticException e) {
            throw new IllegalArgumentException("[" + name + "] can't be converted to milliseconds. ", e);
        }
    }
}
