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

package org.apache.kafka.soak.action;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Identifies a target which contains 0 or more actions.
 */
public final class TargetId {
    public static TargetId parse(String value) {
        int index = value.indexOf(':');
        if (index < 0) {
            return new TargetId(value, null);
        }
        return new TargetId(value.substring(0, index), value.substring(index + 1));
    }

    /**
     * The name of the target action.
     */
    private final String type;

    /**
     * The target node, or null if this target has global scope.
     */
    private final String scope;

    public TargetId(String type) {
        this.type = Objects.requireNonNull(type);
        this.scope = null;
    }

    public TargetId(String type, String scope) {
        this.type = Objects.requireNonNull(type);
        this.scope = scope;
    }

    public String type() {
        return type;
    }

    public String scope() {
        return scope;
    }

    public boolean hasGlobalScope() {
        return scope == null;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(type, scope);
    }

    public Set<ActionId> toActionIds(Collection<String> scopes) {
        HashSet<ActionId> results = new HashSet<>();
        if (scope == null) {
            for (String scope : scopes) {
                results.add(new ActionId(type, scope));
            }
        } else {
            results.add(new ActionId(type, scope));
        }
        return results;
    }

    public static Set<ActionId> actionIds(Collection<TargetId> targetIds, Collection<String> scopes) {
        HashSet<ActionId> results = new HashSet<>();
        for (TargetId targetId : targetIds) {
            if (targetId.hasGlobalScope()) {
                for (String scope : scopes) {
                    results.add(new ActionId(targetId.type(), scope));
                }
            } else {
                results.add(new ActionId(targetId.type(), targetId.scope()));
            }
        }
        return results;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TargetId other = (TargetId) o;
        if (!type.equals(other.type)) {
            return false;
        }
        if (scope == null) {
            return other.scope == null;
        } else {
            return scope.equals(other.scope);
        }
    }

    @Override
    public final String toString() {
        if (scope == null) {
            return type;
        } else {
            return type + ":" + scope;
        }
    }
}
