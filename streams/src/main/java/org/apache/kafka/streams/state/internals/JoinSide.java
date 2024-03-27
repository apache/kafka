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
 * An enum representing the side of a join operation.
 * It provides methods to create instances of {@link LeftOrRightValue} based on the side specified.
 */
@SuppressWarnings("unchecked")
public enum JoinSide {
    LEFT("left") {
        /**
         * Create a new {@link LeftOrRightValue} instance with the V1 value as {@code leftValue} and V2 value as null.
         *
         * @param leftValue the left V1 value
         * @param <V1> the type of the value
         * @return a new {@link LeftOrRightValue} instance
         */
        @Override
        public <V, V1, V2> LeftOrRightValue<V1, V2> make(final V leftValue) {
            Objects.requireNonNull(leftValue, "The left join value is null");
            return (LeftOrRightValue<V1, V2>) new LeftOrRightValue<>(leftValue, null);
        }
    },

    RIGHT("right") {
        /**
         * Create a new {@link LeftOrRightValue} instance with the V2 value as {@code rightValue} and V1 value as null.
         *
         * @param rightValue the right V2 value
         * @param <V2> the type of the value
         * @return a new {@link LeftOrRightValue} instance
         */
        @Override
        public <V, V1, V2> LeftOrRightValue<V1, V2> make(final V rightValue) {
            Objects.requireNonNull(rightValue, "The left join value is null");
            return (LeftOrRightValue<V1, V2>) new LeftOrRightValue<>(null, rightValue);
        }

    };

    private final String joinSideName;

    JoinSide(final String joinSideName) {
        this.joinSideName = joinSideName;
    }

    public abstract <V, V1, V2> LeftOrRightValue<V1, V2> make(final V value);

    /**
     * Returns true if this JoinSide represents the left side.
     *
     * @return true if this JoinSide represents the left side, otherwise false
     */
    public boolean isLeftSide() {
        return this == LEFT;
    }

    @Override
    public String toString() {
        return this.joinSideName;
    }
}
