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

import kafka.utils.TestUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import scala.Tuple2;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeakTestingExtension implements BeforeEachCallback, AfterEachCallback {
    private static final Set<String> EXPECTED_THREAD_NAMES = new HashSet<>(
            Arrays.asList("junit-", "JMX", "feature-zk-node-event-process-thread", "ForkJoinPool", "executor-",
                    "metrics-meter-tick-thread", "scala-", "pool-")
    );
    private static final String THREADS_KEY = "threads";

    @Override
    public void beforeEach(ExtensionContext context) {
        getStore(context).put(THREADS_KEY, Thread.getAllStackTraces().keySet());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterEach(ExtensionContext context) {
        Set<Thread> initialThreads = getStore(context).remove(THREADS_KEY, Set.class);
        Tuple2<Set<Thread>, Object> unexpectedThreads = TestUtils.computeUntilTrue(
                () -> unexpectedThreads(initialThreads),
                DEFAULT_MAX_WAIT_MS,
                100L,
                Set::isEmpty
        );

        assertTrue(unexpectedThreads._1.isEmpty(), "Found unexpected threads after executing test: " +
                unexpectedThreads._1.stream().map(Objects::toString).collect(Collectors.joining(", ")));
    }

    private Set<Thread> unexpectedThreads(Set<Thread> initialThreads) {
        Set<Thread> finalThreads = Thread.getAllStackTraces().keySet();

        if (initialThreads.size() != finalThreads.size()) {
            Set<Thread> leakedThreads = new HashSet<>(finalThreads);
            leakedThreads.removeAll(initialThreads);
            return leakedThreads.stream()
                    .filter(t -> {
                        for (String s: EXPECTED_THREAD_NAMES) {
                            if (t.getName().contains(s))
                                return false;
                        }
                        return true;
                    })
                    .collect(Collectors.toSet());
        }

        return Collections.emptySet();
    }

    private Store getStore(ExtensionContext context) {
        return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
    }
}
