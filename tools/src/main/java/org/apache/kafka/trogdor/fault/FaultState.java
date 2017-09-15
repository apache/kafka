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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.kafka.trogdor.common.JsonUtil;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "stateName")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DoneState.class, name = "done"),
        @JsonSubTypes.Type(value = PendingState.class, name = "pending"),
        @JsonSubTypes.Type(value = RunningState.class, name = "running"),
        @JsonSubTypes.Type(value = SendingState.class, name = "sending")
    })
public abstract class FaultState {
    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return toString().equals(o.toString());
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(toString());
    }

    @Override
    public final String toString() {
        return JsonUtil.toJsonString(this);
    }
}
