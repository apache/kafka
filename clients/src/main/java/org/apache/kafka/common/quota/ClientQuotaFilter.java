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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Describes a client quota entity filter.
 */
public class ClientQuotaFilter {

    private final Collection<ClientQuotaFilterComponent> components;
    private final boolean strict;

    /**
     * A filter to be applied to matching client quotas.
     *
     * @param components the components to filter on
     * @param strict whether the filter only includes specified components
     */
    private ClientQuotaFilter(Collection<ClientQuotaFilterComponent> components, boolean strict) {
        this.components = components;
        this.strict = strict;
    }

    /**
     * Constructs and returns a quota filter that matches all provided components. Matching entities
     * with entity types that are not specified by a component will also be included in the result.
     *
     * @param components the components for the filter
     */
    public static ClientQuotaFilter contains(Collection<ClientQuotaFilterComponent> components) {
        return new ClientQuotaFilter(components, false);
    }

    /**
     * Constructs and returns a quota filter that matches all provided components. Matching entities
     * with entity types that are not specified by a component will *not* be included in the result.
     *
     * @param components the components for the filter
     */
    public static ClientQuotaFilter containsOnly(Collection<ClientQuotaFilterComponent> components) {
        return new ClientQuotaFilter(components, true);
    }

    /**
     * Constructs and returns a quota filter that matches all configured entities.
     */
    public static ClientQuotaFilter all() {
        return new ClientQuotaFilter(Collections.emptyList(), false);
    }

    /**
     * @return the filter's components
     */
    public Collection<ClientQuotaFilterComponent> components() {
        return this.components;
    }

    /**
     * @return whether the filter is strict, i.e. only includes specified components
     */
    public boolean strict() {
        return this.strict;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientQuotaFilter that = (ClientQuotaFilter) o;
        return Objects.equals(components, that.components) && Objects.equals(strict, that.strict);
    }

    @Override
    public int hashCode() {
        return Objects.hash(components, strict);
    }

    @Override
    public String toString() {
        return "ClientQuotaFilter(components=" + components + ", strict=" + strict + ")";
    }
}
