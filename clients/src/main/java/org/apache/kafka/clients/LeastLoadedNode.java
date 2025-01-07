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
package org.apache.kafka.clients;

import org.apache.kafka.common.Node;

public class LeastLoadedNode {
    private final Node node;
    private final boolean atLeastOneConnectionReady;

    public LeastLoadedNode(Node node, boolean atLeastOneConnectionReady) {
        this.node = node;
        this.atLeastOneConnectionReady = atLeastOneConnectionReady;
    }

    public Node node() {
        return node;
    }

    /**
     * Indicates if the least loaded node is available or at least a ready connection exists.
     *
     * <p>There may be no node available while ready connections to live nodes exist. This may happen when
     * the connections are overloaded with in-flight requests. This function takes this into account.
     */
    public boolean hasNodeAvailableOrConnectionReady() {
        return node != null || atLeastOneConnectionReady;
    }
}
