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
package org.apache.kafka.streams.state.internals.utils;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.state.internals.AbstractTransactionalStore;

public class TransactionalStateStores {

    public static void checkSegmentDirs(final boolean transactional,
                                        final Set<String> expectedSegments,
                                        final Set<String> actualSegments) {
        final Set<String> expected;
        if (transactional) {
            expected = expectedSegments.stream()
                .flatMap(segment -> Stream.of(segment, segment + AbstractTransactionalStore.TMP_SUFFIX))
                .collect(Collectors.toSet());
        } else {
            expected = expectedSegments;
        }
        assertEquals(expected, actualSegments);
    }

    public static void checkSegmentDirs(final boolean transactional,
                                        final List<String> expectedSegments,
                                        final List<String> actualSegments) {
        final List<String> expected;
        if (transactional) {
            expected = expectedSegments.stream()
                .flatMap(segment -> Stream.of(segment, segment + ".tmp"))
                .collect(Collectors.toList());
        } else {
            expected = expectedSegments;
        }
        assertEquals(expected, actualSegments);
    }
}
