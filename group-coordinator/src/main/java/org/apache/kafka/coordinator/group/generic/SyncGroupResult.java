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

import org.apache.kafka.common.protocol.Errors;

import java.util.Optional;

/**
 * The result that is wrapped into a {@link org.apache.kafka.common.requests.SyncGroupResponse}
 * to a client's {@link org.apache.kafka.common.requests.SyncGroupRequest}
 */
public class SyncGroupResult {
    private final Optional<String> protocolType;
    private final Optional<String> protocolName;
    private final byte[] memberAssignment;
    private final Errors error;

    public SyncGroupResult(Optional<String> protocolType,
                           Optional<String> protocolName,
                           byte[] memberAssignment,
                           Errors error) {

        this.protocolType = protocolType;
        this.protocolName = protocolName;
        this.memberAssignment = memberAssignment;
        this.error = error;
    }

    public Optional<String> protocolType() {
        return protocolType;
    }

    public Optional<String> protocolName() {
        return protocolName;
    }

    public byte[] memberAssignment() {
        return memberAssignment;
    }

    public Errors error() {
        return error;
    }
}
