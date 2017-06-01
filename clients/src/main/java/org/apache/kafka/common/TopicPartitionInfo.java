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

package org.apache.kafka.common;

import org.apache.kafka.common.utils.Utils;

import java.util.List;

public class TopicPartitionInfo {
    private final int partition;
    private final Node leader;
    private final List<Node> replicas;
    private final List<Node> isr;

    public TopicPartitionInfo(int partition, Node leader, List<Node> replicas, List<Node> isr) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.isr = isr;
    }

    public int partition() {
        return partition;
    }

    public Node leader() {
        return leader;
    }

    public List<Node> replicas() {
        return replicas;
    }

    public List<Node> isr() {
        return isr;
    }

    public String toString() {
        return "(partition=" + partition + ", leader=" + leader + ", replicas=" +
            Utils.join(replicas, ", ") + ", isr=" + Utils.join(isr, ", ") + ")";
    }
}
