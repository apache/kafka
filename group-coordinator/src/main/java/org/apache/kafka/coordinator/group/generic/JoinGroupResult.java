/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.coordinator.group.generic;

import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.Optional;

/**
 * The result that is wrapped into a {@link org.apache.kafka.common.requests.JoinGroupResponse}
 * to a client's {@link org.apache.kafka.common.requests.JoinGroupRequest}
 */
public class JoinGroupResult {
    private final List<JoinGroupResponseData.JoinGroupResponseMember> members;
    private final String memberId;
    private final int generationId;
    private final Optional<String> protocolType;
    private final Optional<String> protocolName;
    private final String leaderId;
    private final boolean skipAssignment;
    private final Errors error;

    public JoinGroupResult(List<JoinGroupResponseData.JoinGroupResponseMember> members,
                           String memberId,
                           int generationId,
                           Optional<String> protocolType,
                           Optional<String> protocolName,
                           String leaderId,
                           boolean skipAssignment,
                           Errors error) {

        this.members = members;
        this.memberId = memberId;
        this.generationId = generationId;
        this.protocolType = protocolType;
        this.protocolName = protocolName;
        this.leaderId = leaderId;
        this.skipAssignment = skipAssignment;
        this.error = error;
    }

    public List<JoinGroupResponseData.JoinGroupResponseMember> members() {
        return members;
    }

    public String memberId() {
        return memberId;
    }

    public int generationId() {
        return generationId;
    }

    public Optional<String> protocolType() {
        return protocolType;
    }

    public Optional<String> protocolName() {
        return protocolName;
    }

    public String leaderId() {
        return leaderId;
    }

    public boolean skipAssignment() {
        return skipAssignment;
    }

    public Errors error() {
        return error;
    }
}
