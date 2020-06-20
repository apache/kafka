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
 * Describes a component for applying a client quota filter.
 */
public class ClientQuotaFilterComponent {

    private final String entityType;
    private final Optional<String> match;

    /**
     * A filter to be applied.
     *
     * @param entityType the entity type the filter component applies to
     * @param match if present, the name that's matched exactly
     *              if empty, matches the default name
     *              if null, matches any specified name
     */
    private ClientQuotaFilterComponent(String entityType, Optional<String> match) {
        this.entityType = Objects.requireNonNull(entityType);
        this.match = match;
    }

    /**
     * Constructs and returns a filter component that exactly matches the provided entity
     * name for the entity type.
     *
     * @param entityType the entity type the filter component applies to
     * @param entityName the entity name that's matched exactly
     */
    public static ClientQuotaFilterComponent ofEntity(String entityType, String entityName) {
        return new ClientQuotaFilterComponent(entityType, Optional.of(Objects.requireNonNull(entityName)));
    }

    /**
     * Constructs and returns a filter component that matches the built-in default entity name
     * for the entity type.
     *
     * @param entityType the entity type the filter component applies to
     */
    public static ClientQuotaFilterComponent ofDefaultEntity(String entityType) {
        return new ClientQuotaFilterComponent(entityType, Optional.empty());
    }

    /**
     * Constructs and returns a filter component that matches any specified name for the
     * entity type.
     *
     * @param entityType the entity type the filter component applies to
     */
    public static ClientQuotaFilterComponent ofEntityType(String entityType) {
        return new ClientQuotaFilterComponent(entityType, null);
    }

    /**
     * @return the component's entity type
     */
    public String entityType() {
        return this.entityType;
    }

    /**
     * @return the optional match string, where:
     *         if present, the name that's matched exactly
     *         if empty, matches the default name
     *         if null, matches any specified name
     */
    public Optional<String> match() {
        return this.match;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientQuotaFilterComponent that = (ClientQuotaFilterComponent) o;
        return Objects.equals(that.entityType, entityType) && Objects.equals(that.match, match);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityType, match);
    }

    @Override
    public String toString() {
        return "ClientQuotaFilterComponent(entityType=" + entityType + ", match=" + match + ")";
    }
}
