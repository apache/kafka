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

import org.apache.kafka.streams.DslStoreFormat;

import java.util.Objects;

/**
 * {@code DslKeyValueParams} is a wrapper class for all parameters that function
 * as inputs to {@link DslStoreSuppliers#keyValueStore(DslKeyValueParams)}.
 */
public class DslKeyValueParams {

    private final String name;
    private final boolean isTimestamped;
    private final DslStoreFormat dslStoreFormat;

    /**
     * @deprecated Since 4.3. Use {@link #DslKeyValueParams(String, DslStoreFormat)} instead.
     * @param name          the name of the store (cannot be {@code null})
     * @param isTimestamped whether the returned stores should be timestamped, see ({@link TimestampedKeyValueStore}
     */
    @Deprecated
    public DslKeyValueParams(final String name, final boolean isTimestamped) {
        Objects.requireNonNull(name);
        this.name = name;
        this.isTimestamped = isTimestamped;
        // If isTimestamped is false and the user is still calling the old deprecated constructor, we should assume they mean plain.
        this.dslStoreFormat = isTimestamped ? DslStoreFormat.TIMESTAMPED : DslStoreFormat.PLAIN;
    }

    /**
     * @param name           the name of the store (cannot be {@code null})
     * @param dslStoreFormat the format of the state store, see ({@link DslStoreFormat}
     */
    public DslKeyValueParams(final String name, final DslStoreFormat dslStoreFormat) {
        this.name =  Objects.requireNonNull(name);
        this.dslStoreFormat = Objects.requireNonNull(dslStoreFormat);
        this.isTimestamped = dslStoreFormat == DslStoreFormat.TIMESTAMPED;
    }

    public String name() {
        return name;
    }

    /**
     * @deprecated Since 4.3. Use {@link #dslStoreFormat()} instead to check the store format.
     * @return {@code true} if the store format is {@link DslStoreFormat#TIMESTAMPED}, {@code false} otherwise
     */
    @Deprecated
    public boolean isTimestamped() {
        return dslStoreFormat == DslStoreFormat.TIMESTAMPED;
    }

    /**
     * Returns the store format for this key-value store.
     *
     * @return the {@link DslStoreFormat} specifying whether to use plain, timestamped, or headers-aware stores
     */
    public DslStoreFormat dslStoreFormat() {
        return dslStoreFormat;
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
                && dslStoreFormat == that.dslStoreFormat
                && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, isTimestamped, dslStoreFormat);
    }

    @Override
    public String toString() {
        return "DslKeyValueParams{" +
                "name='" + name + '\'' +
                "isTimestamped=" + isTimestamped +
                "dslStoreFormat=" + dslStoreFormat +
                '}';
    }
}