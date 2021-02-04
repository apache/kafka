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
package org.apache.kafka.connect.runtime.distributed;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.COMPATIBLE;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.EAGER;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.SESSIONED;


/**
 * This class implements a group protocol for Kafka Connect workers that support incremental and
 * cooperative rebalancing of connectors and tasks. It includes the format of worker state used when
 * joining the group and distributing assignments, and the format of assignments of connectors
 * and tasks to workers.
 */
public class IncrementalCooperativeConnectProtocol {
    /**
     * Returns the collection of Connect protocols that are supported by this version along
     * with their serialized metadata. The protocols are ordered by preference.
     *
     * @param workerState the current state of the worker metadata
     * @param sessioned whether the {@link ConnectProtocolCompatibility#SESSIONED} protocol should
     *                  be included in the collection of supported protocols
     * @return the collection of Connect protocol metadata
     */
    public static JoinGroupRequestProtocolCollection metadataRequest(ExtendedWorkerState workerState, boolean sessioned) {
        // Order matters in terms of protocol preference
        List<JoinGroupRequestProtocol> joinGroupRequestProtocols = new ArrayList<>();
        if (sessioned) {
            joinGroupRequestProtocols.add(new JoinGroupRequestProtocol()
                .setName(SESSIONED.protocol())
                .setMetadata(ExtendedWorkerState.toByteBuffer(workerState, true).array())
            );
        }
        joinGroupRequestProtocols.add(new JoinGroupRequestProtocol()
                        .setName(COMPATIBLE.protocol())
                        .setMetadata(ExtendedWorkerState.toByteBuffer(workerState, false).array())
        );
        joinGroupRequestProtocols.add(new JoinGroupRequestProtocol()
                        .setName(EAGER.protocol())
                        .setMetadata(WorkerState.toByteBuffer(workerState).array())
        );
        return new JoinGroupRequestProtocolCollection(joinGroupRequestProtocols.iterator());
    }
}
