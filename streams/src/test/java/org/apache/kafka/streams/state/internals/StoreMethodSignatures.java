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
package org.apache.kafka.streams.state.internals;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reflection helpers shared by the store completeness tests (KAFKA-20328).
 * <p>
 * A wrapper (adapter or decorator) that does not override a store interface method falls back to the
 * interface default, which cannot reach the wrapped store: it either throws
 * {@link UnsupportedOperationException} (e.g. {@code WindowStore#backwardFetch}) or silently stops
 * delegating (e.g. {@code readOnly} returning {@code this}). Completeness tests use these helpers to
 * turn such a dropped override into a test failure instead of a bug found by manual testing.
 * <p>
 * Signatures use erased parameter types, so a key ({@code K}) or value ({@code V}) appears as
 * {@code Object}.
 */
public final class StoreMethodSignatures {

    private StoreMethodSignatures() {
    }

    /** Public, non-static, non-synthetic methods of {@code type}, including inherited ones. */
    public static Stream<Method> instanceMethods(final Class<?> type) {
        return Arrays.stream(type.getMethods())
            .filter(m -> !m.isSynthetic())
            .filter(m -> !Modifier.isStatic(m.getModifiers()));
    }

    /**
     * Signatures of the {@code storeInterface} methods that {@code impl} resolves to an interface default,
     * i.e. that no class in {@code impl}'s hierarchy overrides. For a generic method, an override is
     * reached via its compiler-generated bridge, which has the erased signature.
     */
    public static Set<String> inheritedInterfaceDefaults(final Class<?> impl, final Class<?> storeInterface) {
        return instanceMethods(storeInterface)
            .filter(m -> resolve(impl, m).getDeclaringClass().isInterface())
            .map(StoreMethodSignatures::signature)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    public static String signature(final Method m) {
        return m.getName() + "(" + Arrays.stream(m.getParameterTypes())
            .map(Class::getSimpleName)
            .collect(Collectors.joining(",")) + ")";
    }

    private static Method resolve(final Class<?> impl, final Method method) {
        try {
            return impl.getMethod(method.getName(), method.getParameterTypes());
        } catch (final NoSuchMethodException e) {
            throw new AssertionError(impl.getName() + " does not expose " + signature(method), e);
        }
    }
}
