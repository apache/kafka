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

package org.apache.kafka.jmx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class JmxObjectConfig {
    private final String name;
    private final String shortName;
    private final List<String> attributes;
    private final ObjectName objectName;

    @JsonCreator
    public JmxObjectConfig(@JsonProperty("name") String name,
                           @JsonProperty("shortName") String shortName,
                           @JsonProperty("attributes") List<String> attributes) throws Exception {
        this.name = (name == null) ? "" : name;
        this.shortName = (shortName == null) ? "" : shortName;
        this.attributes = (attributes == null) ? Collections.emptyList() : new ArrayList<>(attributes);
        this.objectName = new ObjectName(this.name);
    }

    @JsonProperty
    public String name() {
        return name;
    }

    @JsonProperty
    public String shortName() {
        return shortName;
    }

    @JsonProperty
    public List<String> attributes() {
        return attributes;
    }

    ObjectName objectName() {
        return objectName;
    }
}

