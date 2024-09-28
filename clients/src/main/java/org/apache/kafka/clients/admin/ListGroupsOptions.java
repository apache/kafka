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

package org.apache.kafka.clients.admin;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link Admin#listGroups()}.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListGroupsOptions extends AbstractOptions<ListGroupsOptions> {

    private Set<GroupType> types = Collections.emptySet();

    /**
     * If types is set, only groups of these types will be returned by listGroups().
     * Otherwise, all groups are returned.
     */
    public ListGroupsOptions withTypes(Set<GroupType> types) {
        this.types = (types == null || types.isEmpty()) ? Collections.emptySet() : new HashSet<>(types);
        return this;
    }

    /**
     * Returns the list of group types that are requested or empty if no types have been specified.
     */
    public Set<GroupType> types() {
        return types;
    }
}
