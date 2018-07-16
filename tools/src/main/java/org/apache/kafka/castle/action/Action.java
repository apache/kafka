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

package org.apache.kafka.castle.action;

import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An action which the Castle tool can execute.
 */
public abstract class Action {
    /**
     * The ID of this action.
     */
    private final ActionId id;

    /**
     * Action IDs which must be run before this action, if they are to run.
     */
    private final Set<TargetId> comesAfter;

    /**
     * Action types which are run in whenever we run this action.
     */
    private final Set<String> contains;

    /**
     * The initial delay in milliseconds to use.
     */
    private final int initialDelayMs;

    public Action(ActionId id, TargetId[] comesAfter, String[] contains, int initialDelayMs) {
        this.id = id;
        this.comesAfter = Collections.
            unmodifiableSet(new HashSet<>(Arrays.asList(comesAfter)));
        this.contains = Collections.
            unmodifiableSet(new HashSet<>(Arrays.asList(contains)));
        this.initialDelayMs = initialDelayMs;
    }

    /**
     * Run the aciton.
     */
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {}

    public ActionId id() {
        return id;
    }

    /**
     * Return the targets that this Action should comes after.
     */
    public Set<TargetId> comesAfter() {
        return comesAfter;
    }

    /**
     * Return the action types that this Action should contain.
     */
    public Set<String> contains() {
        return contains;
    }

    /**
     * Get the initial delay in milliseconds.
     */
    public int initialDelayMs() {
        return initialDelayMs;
    }

    /**
     * Return the action IDs that this Action should contain.
     */
    public Set<ActionId> containedIds() {
        HashSet<ActionId> containedIds = new HashSet<>();
        for (String type : contains) {
            containedIds.add(new ActionId(type, id.scope()));
        }
        return containedIds;
    }

    @Override
    public final int hashCode() {
        return id.hashCode();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Action other = (Action) o;
        return id.equals(other.id);
    }

    @Override
    public final String toString() {
        return id.toString();
    }
}
