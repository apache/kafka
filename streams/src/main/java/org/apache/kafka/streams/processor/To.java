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
package org.apache.kafka.streams.processor;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * This class is used to provide the optional parameters when sending output records to downstream processor
 * using {@link ProcessorContext#forward(Object, Object, To)}.
 */
public class To {
    protected String childName;
    protected long timestamp;
    protected Set<String> childExclusions;

    private To(final String childName,
               final long timestamp,
               final Set<String> childExclusions) {
        this.childName = childName;
        this.timestamp = timestamp;
        this.childExclusions = childExclusions;
    }

    protected To(final To to) {
        this(to.childName, to.timestamp, to.childExclusions);
    }

    protected void update(final To to) {
        childName = to.childName;
        timestamp = to.timestamp;
        childExclusions = to.childExclusions;
    }

    /**
     * Forward the key/value pair to one of the downstream processors designated by the downstream processor name.
     * @param childName name of downstream processor
     * @return a new {@link To} instance configured with {@code childName}
     */
    public static To child(final String childName) {
        return new To(childName, -1, Collections.emptySet());
    }

    /**
     * Forward the key/value pair to all downstream processors
     * @return a new {@link To} instance configured for all downstream processor
     */
    public static To all() {
        return new To(null, -1,  Collections.emptySet());
    }

    /**
     * Set the timestamp of the output record.
     * @param timestamp the output record timestamp
     * @return itself (i.e., {@code this})
     */
    public To withTimestamp(final long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public To withExclusions(final Set<String> childExclusions) {
        this.childExclusions = childExclusions;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final To to = (To) o;
        return timestamp == to.timestamp &&
            Objects.equals(childName, to.childName) &&
            Objects.equals(childExclusions, to.childExclusions);
    }

    /**
     * Equality is implemented in support of tests, *not* for use in Hash collections, since this class is mutable.
     */
    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("To is unsafe for use in Hash collections");
    }

}
