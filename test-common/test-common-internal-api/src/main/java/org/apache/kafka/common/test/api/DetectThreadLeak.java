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
package org.apache.kafka.common.test.api;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface DetectThreadLeak {
    /**
     * @return the new threads after `DetectThreadLeak` is created
     */
    List<Thread> newThreads();

    /**
     * Creates an instance of {@link DetectThreadLeak} that filters threads based on a given predicate.
     * This method captures the current state of threads that match the predicate at the time of invocation
     * and provides a way to detect new threads that match the predicate but were not present at the initial capture.
     *
     * @param predicate A {@link Predicate<Thread>} used to filter threads. Only threads that satisfy
     *                  the predicate are considered for detection.
     * @return An instance of {@link DetectThreadLeak} that can be used to detect new threads matching
     *         the predicate that were not present at the time of this method's invocation.
     *         The {@link DetectThreadLeak#newThreads()} method of the returned instance will return a list
     *         of new threads that match the predicate and have been started after this method was called.
     */
    static DetectThreadLeak of(Predicate<Thread> predicate) {
        Set<Long> before = Thread.getAllStackTraces().keySet()
                .stream().filter(predicate).map(Thread::getId).collect(Collectors.toSet());
        return () -> Thread.getAllStackTraces().keySet()
                .stream().filter(predicate)
                .filter(t -> !before.contains(t.getId()))
                .toList();
    }
}
