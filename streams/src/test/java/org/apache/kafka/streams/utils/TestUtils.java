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
package org.apache.kafka.streams.utils;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.KafkaStreams;

import org.junit.jupiter.api.TestInfo;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestUtils {
    /**
     * Waits for the given {@link KafkaStreams} instances to all be in a specific {@link KafkaStreams.State}.
     * This method uses polling, which can be more error prone and slightly slower.
     *
     * @param streamsList the list of streams instances to run.
     * @param state the expected state that all the streams to be in within timeout
     * @param timeout the time to wait for the streams to all be in the specific state.
     *
     * @throws InterruptedException if the streams doesn't change to the expected state in time.
     */
    public static void waitForApplicationState(final List<KafkaStreams> streamsList,
                                               final KafkaStreams.State state,
                                               final Duration timeout) throws InterruptedException {
        retryOnExceptionWithTimeout(timeout.toMillis(), () -> {
            final Map<KafkaStreams, KafkaStreams.State> streamsToStates = streamsList
                .stream()
                .collect(Collectors.toMap(stream -> stream, KafkaStreams::state));

            final Map<KafkaStreams, KafkaStreams.State> wrongStateMap = streamsToStates.entrySet()
                .stream()
                .filter(entry -> entry.getValue() != state)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final String reason = String.format(
                "Expected all streams instances in %s to be %s within %d ms, but the following were not: %s",
                streamsList,
                state,
                timeout.toMillis(),
                wrongStateMap
            );
            assertThat(reason, wrongStateMap.isEmpty());
        });
    }

    public static String safeUniqueTestName(final TestInfo testInfo) {
        final String methodName = testInfo.getTestMethod().map(Method::getName).orElse("unknownMethodName");
        return safeUniqueTestName(methodName);
    }

    private static String safeUniqueTestName(final String testName) {
        return sanitize(testName + Uuid.randomUuid().toString());
    }

    private static String sanitize(final String str) {
        return str
            // The `-` is used in Streams' thread name as a separator and some tests rely on this.
            .replace('-', '_')
            .replace(':', '_')
            .replace('.', '_')
            .replace('[', '_')
            .replace(']', '_')
            .replace(' ', '_')
            .replace('=', '_');
    }
}
