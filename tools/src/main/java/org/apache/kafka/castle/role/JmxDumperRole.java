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

package org.apache.kafka.castle.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.jmx.JmxDumpersConfig;
import org.apache.kafka.castle.action.Action;
import org.apache.kafka.castle.action.JmxDumperStartAction;
import org.apache.kafka.castle.action.JmxDumperStatusAction;
import org.apache.kafka.castle.action.JmxDumperStopAction;

import java.util.ArrayList;
import java.util.Collection;

public class JmxDumperRole implements Role {
    public static final String CLASS_NAME = "org.apache.kafka.jmx.JmxDumper";

    private final int initialDelayMs;

    private final JmxDumpersConfig conf;

    @JsonCreator
    public JmxDumperRole(@JsonProperty("initialDelayMs") int initialDelayMs,
                         @JsonProperty("conf") JmxDumpersConfig conf) {
        this.initialDelayMs = initialDelayMs;
        this.conf = (conf == null) ? new JmxDumpersConfig() : conf;
    }

    @Override
    @JsonProperty
    public int initialDelayMs() {
        return initialDelayMs;
    }

    @JsonProperty
    public JmxDumpersConfig conf() {
        return conf;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new JmxDumperStartAction(nodeName, this));
        actions.add(new JmxDumperStatusAction(nodeName, this));
        actions.add(new JmxDumperStopAction(nodeName, this));
        return actions;
    }
};
