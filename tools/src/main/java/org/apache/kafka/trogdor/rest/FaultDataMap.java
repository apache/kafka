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

package org.apache.kafka.trogdor.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.fault.FaultSpec;
import org.apache.kafka.trogdor.fault.FaultState;

import java.util.Map;
import java.util.Objects;

/**
 * Response to GET /faults
 */
public class FaultDataMap {
    private final Map<String, FaultData> faults;

    public static class FaultData  {
        private final FaultSpec spec;
        private final FaultState state;

        @JsonCreator
        public FaultData(@JsonProperty("spec") FaultSpec spec,
                @JsonProperty("state") FaultState state) {
            this.spec = spec;
            this.state = state;
        }

        @JsonProperty
        public FaultSpec spec() {
            return spec;
        }

        @JsonProperty
        public FaultState state() {
            return state;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FaultData that = (FaultData) o;
            return Objects.equals(spec, that.spec) &&
                Objects.equals(state, that.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(spec, state);
        }
    }

    @JsonCreator
    public FaultDataMap(@JsonProperty("faults") Map<String, FaultData> faults) {
        this.faults = faults;
    }

    @JsonProperty
    public Map<String, FaultData> faults() {
        return faults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FaultDataMap that = (FaultDataMap) o;
        return Objects.equals(faults, that.faults);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(faults);
    }

    @Override
    public String toString() {
        return JsonUtil.toJsonString(this);
    }
}
