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

import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.streams.kstream.EmitStrategy;

/**
 * {@code DslSessionParams} is a wrapper class for all parameters that function
 * as inputs to {@link DslStoreSuppliers#sessionStore(DslSessionParams)}.
 */
public class DslSessionParams {

    private final String name;
    private final Duration retentionPeriod;
    private final EmitStrategy emitStrategy;

    /**
     * @param name              name of the store (cannot be {@code null})
     * @param retentionPeriod   length of time to retain data in the store (cannot be negative)
     *                          (note that the retention period must be at least as long enough to
     *                          contain the inactivity gap of the session and the entire grace period.)
     * @param emitStrategy      defines how to emit results
     */
    public DslSessionParams(
            final String name,
            final Duration retentionPeriod,
            final EmitStrategy emitStrategy
    ) {
        Objects.requireNonNull(name);
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.emitStrategy = emitStrategy;
    }

    public String name() {
        return name;
    }

    public Duration retentionPeriod() {
        return retentionPeriod;
    }

    public EmitStrategy emitStrategy() {
        return emitStrategy;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DslSessionParams that = (DslSessionParams) o;
        return Objects.equals(name, that.name)
                && Objects.equals(retentionPeriod, that.retentionPeriod)
                && Objects.equals(emitStrategy, that.emitStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, retentionPeriod, emitStrategy);
    }

    @Override
    public String toString() {
        return "DslSessionParams{" +
                "name='" + name + '\'' +
                ", retentionPeriod=" + retentionPeriod +
                ", emitStrategy=" + emitStrategy +
                '}';
    }
}