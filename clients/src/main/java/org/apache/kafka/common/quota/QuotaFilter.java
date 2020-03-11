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

package org.apache.kafka.common.quota;

import java.util.Objects;
import java.util.Optional;

/**
 * Describes a quota entity filter.
 */
public class QuotaFilter {

    private final String entityType;
    private final Optional<String> match;

    /**
     * A filter to be applied.
     *
     * @param entityType the entity type the filter applies to
     * @param if specified, match the name that's matched exactly
     *        if empty, matches any specified name
     *        if null, matches no specified name
     */
    private QuotaFilter(String entityType, Optional<String> match) {
        this.entityType = Objects.requireNonNull(entityType);
        this.match = match;
    }

    /**
     * Constructs and returns a quota filter that matches the provided entity
     * name for the entity type exactly.
     *
     * @param entityType the entity type the filter applies to
     * @param entityName the entity name that's matched exactly
     */
    public static QuotaFilter matchExact(String entityType, String entityName) {
        return new QuotaFilter(entityType, Optional.of(Objects.requireNonNull(entityName)));
    }

    /**
     * Constructs and returns a quota filter that matches any specified name for
     * the entity type.
     *
     * @param entityType the entity type the filter applies to
     */
    public static QuotaFilter matchSome(String entityType) {
        return new QuotaFilter(entityType, Optional.empty());
    }

    /**
     * Constructs and returns a quota filter that matches no entity name for the
     * entity type exactly.
     *
     * @param entityType the entity type the filter applies to
     */
    public static QuotaFilter matchNone(String entityType) {
        return new QuotaFilter(entityType, null);
    }

    /**
     * @return the entity type the filter applies to
     */
    public String entityType() {
        return this.entityType;
    }

    /**
     * @return whether to match the exact entity name
     */
    public boolean isMatchExact() {
        return this.match != null && this.match.isPresent();
    }

    /**
     * @return the string that's matched exactly
     */
    public String matchExact() {
        return this.match.get();
    }

    /**
     * @return whether to match the exact entity name
     */
    public boolean isMatchSome() {
        return this.match != null && !this.match.isPresent();
    }

    /**
     * @return whether to match no entities with the entity type specified
     */
    public boolean isMatchNone() {
        return this.match == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuotaFilter that = (QuotaFilter) o;
        return Objects.equals(entityType, that.entityType) && Objects.equals(match, that.match);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityType, match);
    }

    @Override
    public String toString() {
        return "QuotaFilter(entityType=" + entityType + ", match=" + match + ")";
    }
}
