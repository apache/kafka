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
 * join or on the right side. This {@link LeftOrRightValue} object contains either the V1 value,
 * which is found in the left topic, or V2 value if it is found in the right topic.
 */
public class LeftOrRightValue<V1, V2> {
    private final V1 leftValue;
    private final V2 rightValue;

    private LeftOrRightValue(final V1 leftValue, final V2 rightValue) {
        if (leftValue != null && rightValue != null) {
            throw new IllegalArgumentException("Only one value cannot be null");
        } else if (leftValue == null && rightValue == null) {
            throw new NullPointerException("Only one value can be null");
        }

        this.leftValue = leftValue;
        this.rightValue = rightValue;
    }

    /**
     * Create a new {@link LeftOrRightValue} instance with the V1 value as {@code leftValue} and
     * V2 value as null.
     *
     * @param leftValue the left V1 value
     * @param <V1>      the type of the value
     * @return a new {@link LeftOrRightValue} instance
     */
    public static <V1, V2> LeftOrRightValue<V1, V2> makeLeftValue(final V1 leftValue) {
        return new LeftOrRightValue<>(leftValue, null);
    }

    /**
     * Create a new {@link LeftOrRightValue} instance with the V2 value as {@code rightValue} and
     * V1 value as null.
     *
     * @param rightValue the right V2 value
     * @param <V2>       the type of the value
     * @return a new {@link LeftOrRightValue} instance
     */
    public static <V1, V2> LeftOrRightValue<V1, V2> makeRightValue(final V2 rightValue) {
        return new LeftOrRightValue<>(null, rightValue);
    }

    /**
     * Create a new {@link LeftOrRightValue} instance with the V value as {@code leftValue} if
     * {@code isLeftSide} is True; otherwise {@code rightValue} if {@code isLeftSide} is False.
     *
     * @param value the V value (either V1 or V2 type)
     * @param <V>   the type of the value
     * @return a new {@link LeftOrRightValue} instance
     */
    public static <V> LeftOrRightValue make(final boolean isLeftSide, final V value) {
        Objects.requireNonNull(value, "value is null");
        return isLeftSide
            ? LeftOrRightValue.makeLeftValue(value)
            : LeftOrRightValue.makeRightValue(value);
    }

    public V1 getLeftValue() {
        return leftValue;
    }

    public V2 getRightValue() {
        return rightValue;
    }

    @Override
    public String toString() {
        return "<"
            + ((leftValue != null) ? "left," + leftValue : "right," + rightValue)
            + ">";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final LeftOrRightValue<?, ?> that = (LeftOrRightValue<?, ?>) o;
        return Objects.equals(leftValue, that.leftValue) &&
            Objects.equals(rightValue, that.rightValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftValue, rightValue);
    }
}
