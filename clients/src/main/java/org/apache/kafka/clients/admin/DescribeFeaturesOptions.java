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
package org.apache.kafka.clients.admin;

import java.util.OptionalInt;

/**
 * Options for {@link AdminClient#describeFeatures(DescribeFeaturesOptions)}.
 */
public class DescribeFeaturesOptions extends AbstractOptions<DescribeFeaturesOptions> {
    private OptionalInt nodeId = OptionalInt.empty();

    /**
     * Set the node id to which the request should be sent.
     */
    public DescribeFeaturesOptions nodeId(int nodeId) {
        this.nodeId = OptionalInt.of(nodeId);
        return this;
    }

    /**
     * The node id to which the request should be sent. If the node id is empty, the request will be sent to the
     * arbitrary controller/broker.
     */
    public OptionalInt nodeId() {
        return nodeId;
    }
}
