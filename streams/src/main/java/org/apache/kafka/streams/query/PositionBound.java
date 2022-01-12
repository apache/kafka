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
package org.apache.kafka.streams.query;


import org.apache.kafka.common.annotation.InterfaceStability.Evolving;

import java.util.Objects;

/**
 * A class bounding the processing state {@link Position} during queries. This can be used to
 * specify that a query should fail if the locally available partition isn't caught up to the
 * specified bound. "Unbounded" places no restrictions on the current location of the partition.
 */
@Evolving
public class PositionBound {

    private final Position position;

    private PositionBound(final Position position) {
        this.position = position.copy();
    }

    /**
     * Creates a new PositionBound representing "no bound"
     */
    public static PositionBound unbounded() {
        return new PositionBound(Position.emptyPosition());
    }

    /**
     * Creates a new PositionBound representing a specific position.
     */
    public static PositionBound at(final Position position) {
        return new PositionBound(position);
    }

    /**
     * Returns true iff this object specifies that there is no position bound.
     */
    public boolean isUnbounded() {
        return position.isEmpty();
    }

    /**
     * Returns the specific position of this bound.
     */
    public Position position() {
        return position;
    }

    @Override
    public String toString() {
        return "PositionBound{position=" + position + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PositionBound that = (PositionBound) o;
        return Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException(
            "This mutable object is not suitable as a hash key");
    }
}
