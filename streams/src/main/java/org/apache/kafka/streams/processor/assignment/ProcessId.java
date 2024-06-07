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
package org.apache.kafka.streams.processor.assignment;

import java.util.UUID;

/** A simple wrapper around UUID that abstracts a Process ID */
public class ProcessId implements Comparable<ProcessId> {

    private final UUID id;

    public ProcessId(final UUID id) {
        this.id = id;
    }

    /**
     *
     * @return the underlying {@code UUID} that this ProcessID is wrapping.
     */
    public UUID id() {
        return id;
    }

    @Override
    public String toString() {
        return "ProcessId{id=" + id + "}";
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final ProcessId other = (ProcessId) obj;
        return this.id.equals(other.id());
    }

    @Override
    public int compareTo(final ProcessId o) {
        return this.id.compareTo(o.id);
    }
}