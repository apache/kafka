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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.utils.Utils;

import java.util.TreeMap;
import java.util.Set;

/**
 * The state a fault is in on the controller when it is scheduled to be sent to several agents.
 */
public class SendingState extends FaultState {
    private final TreeMap<String, Boolean> nodes;
    private int remainingNodes;

    public SendingState(@JsonProperty("nodeNames") Set<String> nodeNames) {
        this.nodes = new TreeMap<>();
        for (String nodeName : nodeNames) {
            nodes.put(nodeName, false);
        }
        remainingNodes = nodeNames.size();
    }

    @JsonProperty
    public synchronized Set<String> nodeNames() {
        return nodes.keySet();
    }

    /**
     * Complete a send operation.
     *
     * @param nodeName      The name of the node we sent to.
     * @return              True if there are no more send operations left.
     */
    public synchronized boolean completeSend(String nodeName) {
        if (!nodes.containsKey(nodeName)) {
            throw new RuntimeException("Node " + nodeName + " was not to supposed to " +
                "receive this fault.  The fault was scheduled on nodes: " +
                Utils.join(nodes.keySet(), ", "));
        }
        if (nodes.put(nodeName, true)) {
            throw new RuntimeException("Node " + nodeName + " already received this fault.");
        }
        remainingNodes--;
        return remainingNodes == 0;
    }
}
