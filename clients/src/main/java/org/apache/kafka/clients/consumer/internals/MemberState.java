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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID;

public class MemberState {
    public static final short GROUP_AUTHORIZATION_FAILED = Errors.GROUP_AUTHORIZATION_FAILED.code();
    public static final short NOT_COORDINATOR = Errors.NOT_COORDINATOR.code();
    public static final short COORDINATOR_NOT_AVAILABLE = Errors.COORDINATOR_NOT_AVAILABLE.code();
    public static final short COORDINATOR_LOAD_IN_PROGRESS = Errors.COORDINATOR_LOAD_IN_PROGRESS.code();
    public static final short INVALID_REQUEST = Errors.INVALID_REQUEST.code();
    public static final short UNKNOWN_MEMBER_ID = Errors.UNKNOWN_MEMBER_ID.code();
    public static final short FENCED_MEMBER_EPOCH = Errors.FENCED_MEMBER_EPOCH.code();
    public static final short UNSUPPORTED_ASSIGNOR = Errors.UNSUPPORTED_ASSIGNOR.code();
    public static final short UNRELEASED_INSTANCE_ID = Errors.UNRELEASED_INSTANCE_ID.code();
    public static final short GROUP_MAX_SIZE_REACHED = Errors.GROUP_MAX_SIZE_REACHED.code();

    public final String groupId;
    public final Optional<String> groupInstanceId;
    public final AssignorSelector assignor;
    public Generation generation = Generation.NO_GENERATION;

    Optional<String> memberId;
    int memberEpoch;

    public void reset() {
        this.setMemberEpoch(0);
    }

    public enum State {
        UNJOINED, JOINING, JOINED
    }

    public State state = State.UNJOINED;

    public MemberState(String groupId, Optional<String> groupInstanceId, AssignorSelector assignor) {
        this.groupId = groupId;
        this.groupInstanceId = groupInstanceId;
        this.memberId = Optional.empty();
        this.memberEpoch = 0;
        this.assignor = assignor;
    }

    public MemberState(final GroupRebalanceConfig config, AssignorSelector assignor) {
        this.groupId = config.groupId;
        this.groupInstanceId = config.groupInstanceId;
        this.memberId = Optional.empty();
        this.memberEpoch = 0;
        this.assignor = assignor;
    }

    public void maybeUpdateOnHeartbeatResponse(ConsumerGroupHeartbeatResponseData responseData) {
        short errorCode = responseData.errorCode();

        if (errorCode == Errors.NONE.code()) {
            this.state = State.JOINED;  // Change joining state here
            if (this.memberId.isPresent() && !this.memberId.get().equals(responseData.memberId())) {
                throw new KafkaException("Inconsistent member id returned in heartbeat response: (memberId returned: " +
                    responseData.memberId() + ", current memberId: " + this.memberId.get() + ")");
            }
            this.memberId = Optional.of(responseData.memberId());
            setMemberEpoch(responseData.memberEpoch());
        }
    }

    public int setMemberEpoch(final int newEpoch) {
        this.memberEpoch = newEpoch;
        return this.memberEpoch;
    }

    public void join() {
        this.state = State.JOINING;  // Change joining state here
    }

    public boolean unjoined() {
        return this.state == State.UNJOINED;
    }

    public static class AssignorSelector {
        public enum Type { CLIENT, SERVER }
        public Type type;
        public Object activeAssignor;

        public void setClientSideAssignor(List<Assignor> assignors) {
            this.type = Type.CLIENT;
            this.activeAssignor = assignors;
        }

        public void setServersideAssignor(String assignorConfig) {
            this.type = Type.SERVER;
            this.activeAssignor = assignorConfig;
        }
    }

    public static class Assignor {
        public final String name;
        public final byte reason;
        public final short minVersion;
        public final short maxVersion;
        public final short version;
        public final byte[] metadata;

        public Assignor(String name, byte reason, short minVersion, short maxVersion, short version, byte[] metadata) {
            this.name = name;
            this.reason = reason;
            this.minVersion = minVersion;
            this.maxVersion = maxVersion;
            this.version = version;
            this.metadata = metadata;
        }

        @Override
        public String toString() {
            return "Assignor(name=" + name + ", reason=" + reason + ", minVersion=" + minVersion + ", maxVersion=" + maxVersion + ", version=" + version + ", metadata=" + metadata + ")";
        }

        @Override
        public int hashCode() {
            int result = 1;
            result = result * 59 + (this.name == null ? 43 : this.name.hashCode());
            result = result * 59 + this.reason;
            result = result * 59 + this.minVersion;
            result = result * 59 + this.maxVersion;
            result = result * 59 + this.version;
            result = result * 59 + java.util.Arrays.hashCode(this.metadata);
            return result;
        }
    }

    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
            DEFAULT_GENERATION_ID,
            JoinGroupRequest.UNKNOWN_MEMBER_ID,
            null);

        public final int generationId;
        public final String memberId;
        public final String protocolName;

        public Generation(int generationId, String memberId, String protocolName) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocolName = protocolName;
        }

        /**
         * @return true if this generation has a valid member id, false otherwise. A member might have an id before
         * it becomes part of a group generation.
         */
        public boolean hasMemberId() {
            return !memberId.isEmpty();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Generation that = (Generation) o;
            return generationId == that.generationId &&
                Objects.equals(memberId, that.memberId) &&
                Objects.equals(protocolName, that.protocolName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generationId, memberId, protocolName);
        }

        @Override
        public String toString() {
            return "Generation{" +
                "generationId=" + generationId +
                ", memberId='" + memberId + '\'' +
                ", protocol='" + protocolName + '\'' +
                '}';
        }
    }
}
