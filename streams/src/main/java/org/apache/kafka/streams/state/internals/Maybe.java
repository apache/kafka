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

import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A container that may be empty, may contain null, or may contain a value.
 * Distinct from {@link java.util.Optional<T>}, since Optional cannot contain null.
 *
 * @param <T>
 */
public final class Maybe<T> {
    private final T nullableValue;
    private final boolean defined;

    public static <T> Maybe<T> defined(final T nullableValue) {
        return new Maybe<>(nullableValue);
    }

    public static <T> Maybe<T> undefined() {
        return new Maybe<>();
    }

    private Maybe(final T nullableValue) {
        this.nullableValue = nullableValue;
        defined = true;
    }

    private Maybe() {
        nullableValue = null;
        defined = false;
    }

    public T getNullableValue() {
        if (defined) {
            return nullableValue;
        } else {
            throw new NoSuchElementException();
        }
    }

    public boolean isDefined() {
        return defined;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Maybe<?> maybe = (Maybe<?>) o;

        // All undefined maybes are equal
        // All defined null maybes are equal
        return defined == maybe.defined &&
            (!defined || Objects.equals(nullableValue, maybe.nullableValue));
    }

    @Override
    public int hashCode() {
        // Since all undefined maybes are equal, we can hard-code their hashCode to -1.
        // Since all defined null maybes are equal, we can hard-code their hashCode to 0.
        return defined ? nullableValue == null ? 0 : nullableValue.hashCode() : -1;
    }

    @Override
    public String toString() {
        if (defined) {
            return "DefinedMaybe{" + nullableValue + "}";
        } else {
            return "UndefinedMaybe{}";
        }
    }
}
