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

import org.apache.kafka.common.message.JoinGroupResponseData;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * An assignor that computes a distribution of connectors and tasks among the workers of the group
 * that performs rebalancing.
 */
public interface ConnectAssignor {
    /**
     * Based on the member metadata and the information stored in the worker coordinator this
     * method computes an assignment of connectors and tasks among the members of the worker group.
     *
     * @param leaderId the leader of the group
     * @param protocol the protocol type; for Connect assignors this is normally "connect"
     * @param allMemberMetadata the metadata of all the active workers of the group
     * @param coordinator the worker coordinator that runs this assignor
     * @return the assignment of connectors and tasks to workers
     */
    Map<String, ByteBuffer> performAssignment(String leaderId, String protocol,
                                              List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata,
                                              WorkerCoordinator coordinator);
}
