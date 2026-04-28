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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.protocol.Errors;

import java.util.Optional;

/**
 * Options for {@link Admin#addRaftVoter}.
 *
 * <p>
 * The clusterId is optional.
 * <p>
 * If provided, the request will only succeed if the cluster id matches the id of the current cluster.
 * If the cluster id does not match, the request will fail with {@link Errors#INCONSISTENT_CLUSTER_ID}.
 * <p>
 * If not provided, the cluster id check is skipped.
 */
@InterfaceStability.Stable
public class AddRaftVoterOptions extends AbstractOptions<AddRaftVoterOptions> {
    private Optional<String> clusterId = Optional.empty();

    public AddRaftVoterOptions setClusterId(Optional<String> clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public Optional<String> clusterId() {
        return clusterId;
    }
}
