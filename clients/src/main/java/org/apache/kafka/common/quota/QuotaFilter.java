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

/**
 * Describes a quota entity filter.
 */
public class QuotaFilter {

    // Matches all entities with the entity type specified.
    private static final String MATCH_SPECIFIED = "<specified>";

    private final String entityType;
    private final String match;

    /**
     * A filter to be applied.
     *
     * @param entityType the entity type the filter applies to
     * @param match the string that's matched exactly
     */
    private QuotaFilter(String entityType, String match) {
        this.entityType = Objects.requireNonNull(entityType);
        this.match = Objects.requireNonNull(match);
    }

    /**
     * Constructs and returns a quota filter that matches the provided entity
     * name for the entity type exactly.
     *
     * @param entityType the entity type the filter applies to
     * @param match the string that's matched exactly
     */
    public static QuotaFilter matchExact(String entityType, String entityName) {
        return new QuotaFilter(entityType, entityName);
    }

    /**
     * Constructs and returns a quota filter that matches the any entity name
     * that's specified for the entity type.
     *
     * @param entityType the entity type the filter applies to
     */
    public static QuotaFilter matchSpecified(String entityType) {
        return new QuotaFilter(entityType, MATCH_SPECIFIED);
    }

    /**
     * @return the entity type the filter applies to
     */
    public String entityType() {
        return this.entityType;
    }

    /**
     * @return the string that's matched exactly
     */
    public String match() {
        return this.match;
    }

    /**
     * @return whether to match the exact entity name
     */
    public boolean isMatchExact() {
        return !isMatchSpecified();
    }

    /**
     * @return whether to match all entities with the entity type specified
     */
    public boolean isMatchSpecified() {
        return this.match == MATCH_SPECIFIED;
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
