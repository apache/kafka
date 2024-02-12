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
package org.apache.kafka.connect.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class LoggerLevel {

    private final String level;
    private final Long lastModified;

    public LoggerLevel(
            @JsonProperty("level") String level,
            @JsonProperty("last_modified") Long lastModified
    ) {
        this.level = Objects.requireNonNull(level, "level may not be null");
        this.lastModified = lastModified;
    }

    @JsonProperty
    public String level() {
        return level;
    }

    @JsonProperty("last_modified")
    public Long lastModified() {
        return lastModified;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        LoggerLevel that = (LoggerLevel) o;
        return level.equals(that.level) && Objects.equals(lastModified, that.lastModified);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, lastModified);
    }

    @Override
    public String toString() {
        return "LoggerLevel{"
                + "level='" + level + '\''
                + ", lastModified=" + lastModified
                + '}';
    }
}
