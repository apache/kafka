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

package org.apache.kafka.trogdor.fault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * The specification for a fault that does nothing.
 *
 * This fault type exists mainly to test the fault injection system.
 */
public class NoOpFaultSpec extends AbstractFaultSpec {
    @JsonCreator
    public NoOpFaultSpec(@JsonProperty("startMs") long startMs,
                         @JsonProperty("durationMs") long durationMs) {
        super(startMs, durationMs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NoOpFaultSpec that = (NoOpFaultSpec) o;
        return Objects.equals(startMs(), that.startMs()) &&
            Objects.equals(durationMs(), that.durationMs());
    }

    @Override
    public int hashCode() {
        return Objects.hash(startMs(), durationMs());
    }
}
