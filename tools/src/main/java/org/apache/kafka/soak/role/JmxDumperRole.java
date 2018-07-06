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
import org.apache.kafka.soak.action.JmxDumperStartAction;
import org.apache.kafka.soak.action.JmxDumperStatusAction;
import org.apache.kafka.soak.action.JmxDumperStopAction;
import org.apache.kafka.tools.JmxDumper.TopLevelConfig;

import java.util.ArrayList;
import java.util.Collection;

public class JmxDumperRole implements Role {
    public static final String CLASS_NAME = "org.apache.kafka.tools.JmxDumper";

    private final TopLevelConfig conf;

    @JsonCreator
    public JmxDumperRole(@JsonProperty("conf") TopLevelConfig conf) {
        this.conf = conf == null ? new TopLevelConfig() : conf;
    }

    @JsonProperty
    public TopLevelConfig conf() {
        return conf;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new JmxDumperStartAction(nodeName, conf));
        actions.add(new JmxDumperStatusAction(nodeName));
        actions.add(new JmxDumperStopAction(nodeName));
        return actions;
    }
};
