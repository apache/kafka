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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

/**
 * LeakTester for objects which have some state (open vs closed) that can be examined by a {@link Predicate}.
 * <p>An object is considered leaked if it is passed to {@link #open(Object)}, but at end of the interval the predicate
 * returns true. This tester is only compatible with resources which are never re-opened, and resources which are not
 * closed remain strongly-referenced by this class and may cause memory leaks.
 * @param <T> The type of objects being tracked
 */
public class PredicateLeakTester<T> implements LeakTester {

    private final ConcurrentMap<T, Exception> refs = new ConcurrentHashMap<>();
    private final Predicate<T> isOpen;
    private final Class<T> clazz;

    /**
     * Create a new leak tester based on predicate evaluation.
     * @param isOpen A predicate that returns true if a resource is open, and false otherwise.
     * @param clazz The superclass of objects tracked by this tester
     */
    public PredicateLeakTester(Predicate<T> isOpen, Class<T> clazz) {
        this.isOpen = Objects.requireNonNull(isOpen, "predicate must be non-null");
        this.clazz = Objects.requireNonNull(clazz, "class must be non-null");
    }

    /**
     * Register a resource to be tracked
     * <p>This method captures a stacktrace when a resource is registered, so this method should be called near to
     * where the resource is opened or created. This is included in the stack trace thrown from failed leak assertions.
     * @param obj The resource being tracked, non-null
     * @return The passed-in object, for method-chaining
     */
    public T open(T obj) {
        Objects.requireNonNull(obj, "resource must be non-null");
        try {
            throw new Exception("Opened " + obj.getClass().getName());
        } catch (Exception e) {
            refs.put(obj, e);
        }
        return obj;
    }

    private Set<Exception> live() {
        Set<Exception> ret = new HashSet<>();
        Iterator<Map.Entry<T, Exception>> iterator = refs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<T, Exception> entry = iterator.next();
            if (isOpen.test(entry.getKey())) {
                ret.add(entry.getValue());
            } else {
                iterator.remove();
            }
        }
        return ret;
    }

    /**
     * Start a leak test
     * @return An ongoing leak test
     */
    public LeakTest start() {
        Set<Exception> before = live();
        return () -> {
            Set<Exception> after = live();
            after.removeAll(before);
            if (!after.isEmpty()) {
                AssertionError e = new AssertionError(clazz.getSimpleName() + " instances left open");
                for (Exception leakedSocket : after) {
                    e.addSuppressed(leakedSocket);
                }
                throw e;
            }
        };
    }
}
