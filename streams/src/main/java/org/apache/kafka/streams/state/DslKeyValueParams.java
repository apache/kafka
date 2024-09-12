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
package org.apache.kafka.streams.state;

import java.util.Objects;

/**
 * {@code DslKeyValueParams} is a wrapper class for all parameters that function
 * as inputs to {@link DslStoreSuppliers#keyValueStore(DslKeyValueParams)}.
 */
public class DslKeyValueParams {

    private final String name;
    private final boolean isTimestamped;

    /**
     * @param name          the name of the store (cannot be {@code null})
     * @param isTimestamped whether the returned stores should be timestamped, see ({@link TimestampedKeyValueStore}
     */
    public DslKeyValueParams(final String name, final boolean isTimestamped) {
        Objects.requireNonNull(name);
        this.name = name;
        this.isTimestamped = isTimestamped;
    }

    public String name() {
        return name;
    }

    public boolean isTimestamped() {
        return isTimestamped;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DslKeyValueParams that = (DslKeyValueParams) o;
        return isTimestamped == that.isTimestamped
                && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, isTimestamped);
    }

    @Override
    public String toString() {
        return "DslKeyValueParams{" +
                "name='" + name + '\'' +
                "isTimestamped=" + isTimestamped +
                '}';
    }
}