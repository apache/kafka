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
import org.apache.kafka.soak.action.ZooKeeperStartAction;
import org.apache.kafka.soak.action.ZooKeeperStatusAction;
import org.apache.kafka.soak.action.ZooKeeperStopAction;

import java.util.ArrayList;
import java.util.Collection;

public class ZooKeeperRole implements Role {
    public static final String ZOOKEEPER_CLASS_NAME =
        "org.apache.zookeeper.server.quorum.QuorumPeerMain";

    private final int initialDelayMs;

    @JsonCreator
    public ZooKeeperRole(@JsonProperty("initialDelayMs") int initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
    }

    @Override
    @JsonProperty
    public int initialDelayMs() {
        return initialDelayMs;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new ZooKeeperStartAction(nodeName, this));
        actions.add(new ZooKeeperStatusAction(nodeName, this));
        actions.add(new ZooKeeperStopAction(nodeName, this));
        return actions;
    }
};
