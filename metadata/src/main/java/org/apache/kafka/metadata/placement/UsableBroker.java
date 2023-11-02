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

package org.apache.kafka.metadata.placement;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;
import java.util.Optional;


/**
 * A broker where a replica can be placed.
 */
@InterfaceStability.Unstable
public class UsableBroker {
    private final int id;

    private final Optional<String> rack;

    private final boolean fenced;

    public UsableBroker(int id, Optional<String> rack, boolean fenced) {
        this.id = id;
        this.rack = rack;
        this.fenced = fenced;
    }

    public int id() {
        return id;
    }

    public Optional<String> rack() {
        return rack;
    }

    public boolean fenced() {
        return fenced;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof UsableBroker)) return false;
        UsableBroker other = (UsableBroker) o;
        return other.id == id && other.rack.equals(rack) && other.fenced == fenced;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id,
            rack,
            fenced);
    }

    @Override
    public String toString() {
        return "UsableBroker" +
            "(id=" + id +
            ", rack=" + rack +
            ", fenced=" + fenced +
            ")";
    }
}
