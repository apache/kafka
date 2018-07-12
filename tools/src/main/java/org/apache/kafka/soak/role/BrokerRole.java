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

package org.apache.kafka.soak.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.soak.action.Action;
import org.apache.kafka.soak.action.BrokerStartAction;
import org.apache.kafka.soak.action.BrokerStatusAction;
import org.apache.kafka.soak.action.BrokerStopAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BrokerRole implements Role {
    public static final String KAFKA_CLASS_NAME = "kafka.Kafka";

    private static final String DEFAULT_JVM_PERFORMANCE_OPTS = "-Xmx3g -Xms3g";

    private final int initialDelayMs;

    private final Map<String, String> conf;

    private final String jvmOptions;

    @JsonCreator
    public BrokerRole(@JsonProperty("initialDelayMs") int initialDelayMs,
                      @JsonProperty("conf") Map<String, String> conf,
                      @JsonProperty("jvmOptions") String jvmOptions) {
        this.initialDelayMs = initialDelayMs;
        this.conf = conf == null ? Collections.emptyMap() :
            Collections.unmodifiableMap(new HashMap<>(conf));
        if ((jvmOptions == null) || jvmOptions.isEmpty()) {
            this.jvmOptions = DEFAULT_JVM_PERFORMANCE_OPTS;
        } else {
            this.jvmOptions = jvmOptions;
        }
    }

    @Override
    @JsonProperty
    public int initialDelayMs() {
        return initialDelayMs;
    }

    @JsonProperty
    public Map<String, String> conf() {
        return conf;
    }

    @JsonProperty
    public String jvmOptions() {
        return jvmOptions;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new BrokerStartAction(nodeName, this));
        actions.add(new BrokerStatusAction(nodeName, this));
        actions.add(new BrokerStopAction(nodeName, this));
        return actions;
    }
};
