/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

// Equivalent to o.a.k.copycat.util.ConnectorTaskId, but with annotations for JSON serialization
public class ConnectorTaskId {
    private final String connector;
    private final int task;

    @JsonCreator
    public ConnectorTaskId(String job, int task) {
        this.connector = job;
        this.task = task;
    }

    public ConnectorTaskId(org.apache.kafka.copycat.util.ConnectorTaskId orig) {
        this.connector = orig.connector();
        this.task = orig.task();
    }

    @JsonProperty
    public String connector() {
        return connector;
    }

    @JsonProperty
    public int task() {
        return task;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ConnectorTaskId that = (ConnectorTaskId) o;

        if (task != that.task)
            return false;
        if (connector != null ? !connector.equals(that.connector) : that.connector != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = connector != null ? connector.hashCode() : 0;
        result = 31 * result + task;
        return result;
    }

    @Override
    public String toString() {
        return connector + '-' + task;
    }
}
