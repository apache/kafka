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

import java.util.Collections;

import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.EAGER;

/**
 * This class implements the protocol for Kafka Connect workers in a group. It includes the format of worker state used when
 * joining the group and distributing assignments, and the format of assignments of connectors and tasks to workers.
 */
public class ConnectProtocol {
    public static final int CONNECTOR_TASK = -1;

    /**
     * Returns the collection of Connect protocols that are supported by this version along
     * with their serialized metadata. The protocols are ordered by preference.
     *
     * @param workerState the current state of the worker metadata
     * @return the collection of Connect protocol metadata
     */
    public static JoinGroupRequestProtocolCollection metadataRequest(WorkerState workerState) {
        return new JoinGroupRequestProtocolCollection(Collections.singleton(
                new JoinGroupRequestProtocol()
                        .setName(EAGER.protocol())
                        .setMetadata(WorkerState.toByteBuffer(workerState).array()))
                .iterator());
    }
}
