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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Provides helper methods for assignors.
 */
public final class AssignorHelpers {
    private static final Class<?> UNMODIFIABLE_MAP_CLASS = Collections.unmodifiableMap(new HashMap<>()).getClass();
    private static final Class<?> EMPTY_MAP_CLASS = Collections.emptyMap().getClass();

    /**
     * @return True if the provided map is an UnmodifiableMap or EmptyMap. Those classes are not
     * public hence we cannot use the `instanceof` operator.
     */
    public static boolean isImmutableMap(Map<?, ?> map) {
        return UNMODIFIABLE_MAP_CLASS.isInstance(map) || EMPTY_MAP_CLASS.isInstance(map);
    }

    /**
     * Deep copies a member assignment map.
     * @param map The assignment to copy.
     * @return A deep copy of the assignment.
     */
    public static Map<Uuid, Set<Integer>> deepCopyAssignment(Map<Uuid, Set<Integer>> map) {
        Map<Uuid, Set<Integer>> copy = new HashMap<>(map.size());
        for (Map.Entry<Uuid, Set<Integer>> entry : map.entrySet()) {
            copy.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return copy;
    }
}
