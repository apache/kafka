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

import java.util.Objects;

/**
 * This class is used in combination of {@link KeyAndJoinSide}. The {@link KeyAndJoinSide} class
 * combines a key with a boolean value that specifies if the key is found in the left side of a
 * join or on the right side. This {@link ValueOrOtherValue} object contains either the V1 value,
 * which is found in the left topic, or V2 value if it is found in the right topic.
 */
public class ValueOrOtherValue<V1, V2> {
    private final V1 thisValue;
    private final V2 otherValue;

    private ValueOrOtherValue(final V1 thisValue, final V2 otherValue) {
        this.thisValue = thisValue;
        this.otherValue = otherValue;
    }

    /**
     * Create a new {@link ValueOrOtherValue} instance with the V1 value as {@code thisValue} and
     * V2 value as null.
     *
     * @param thisValue the V1 value
     * @param <V1>      the type of the value
     * @return a new {@link ValueOrOtherValue} instance
     */
    public static <V1, V2> ValueOrOtherValue<V1, V2> makeValue(final V1 thisValue) {
        return new ValueOrOtherValue<>(thisValue, null);
    }

    /**
     * Create a new {@link ValueOrOtherValue} instance with the V2 value as {@code otherValue} and
     * V1 value as null.
     *
     * @param otherValue the V2 value
     * @param <V2>       the type of the value
     * @return a new {@link ValueOrOtherValue} instance
     */
    public static <V1, V2> ValueOrOtherValue<V1, V2> makeOtherValue(final V2 otherValue) {
        return new ValueOrOtherValue<>(null, otherValue);
    }

    public V1 getThisValue() {
        return thisValue;
    }

    public V2 getOtherValue() {
        return otherValue;
    }

    @Override
    public String toString() {
        return "<" + thisValue + "," + otherValue + ">";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ValueOrOtherValue<?, ?> that = (ValueOrOtherValue<?, ?>) o;
        return Objects.equals(thisValue, that.thisValue) &&
            Objects.equals(otherValue, that.otherValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(thisValue, otherValue);
    }
}
