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
package org.apache.kafka.coordinator.group.modern;

import java.util.Set;

/**
 * Abstract member common for group members.
 */
public abstract class ModernGroupMember {

    /**
     * The member id.
     */
    protected String memberId;

    /**
     * The current member epoch.
     */
    protected int memberEpoch;

    /**
     * The previous member epoch.
     */
    protected int previousMemberEpoch;

    /**
     * The member state.
     */
    protected MemberState state;

    /**
     * The instance id provided by the member.
     */
    protected String instanceId;

    /**
     * The rack id provided by the member.
     */
    protected String rackId;

    /**
     * The client id reported by the member.
     */
    protected String clientId;

    /**
     * The host reported by the member.
     */
    protected String clientHost;

    /**
     * The list of subscriptions (topic names) configured by the member.
     */
    protected Set<String> subscribedTopicNames;

    protected ModernGroupMember(
        String memberId,
        int memberEpoch,
        int previousMemberEpoch,
        String instanceId,
        String rackId,
        String clientId,
        String clientHost,
        Set<String> subscribedTopicNames,
        MemberState state
    ) {
        this.memberId = memberId;
        this.memberEpoch = memberEpoch;
        this.previousMemberEpoch = previousMemberEpoch;
        this.state = state;
        this.instanceId = instanceId;
        this.rackId = rackId;
        this.clientId = clientId;
        this.clientHost = clientHost;
        this.subscribedTopicNames = subscribedTopicNames;
    }

    /**
     * @return The member id.
     */
    public String memberId() {
        return memberId;
    }

    /**
     * @return The current member epoch.
     */
    public int memberEpoch() {
        return memberEpoch;
    }

    /**
     * @return The previous member epoch.
     */
    public int previousMemberEpoch() {
        return previousMemberEpoch;
    }

    /**
     * @return The instance id.
     */
    public String instanceId() {
        return instanceId;
    }

    /**
     * @return The rack id.
     */
    public String rackId() {
        return rackId;
    }

    /**
     * @return The client id.
     */
    public String clientId() {
        return clientId;
    }

    /**
     * @return The client host.
     */
    public String clientHost() {
        return clientHost;
    }

    /**
     * @return The list of subscribed topic names.
     */
    public Set<String> subscribedTopicNames() {
        return subscribedTopicNames;
    }

    /**
     * @return The current state.
     */
    public MemberState state() {
        return state;
    }

    /**
     * @return True if the member is in the Stable state and at the desired epoch.
     */
    public boolean isReconciledTo(int targetAssignmentEpoch) {
        return state == MemberState.STABLE && memberEpoch == targetAssignmentEpoch;
    }
}
