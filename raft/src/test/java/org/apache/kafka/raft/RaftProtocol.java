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
package org.apache.kafka.raft;

/**
 * Determines what versions of RPCs are in use. Note, these are ordered from oldest to newest, and are
 * cumulative. E.g. KIP_996_PROTOCOL includes KIP_853_PROTOCOL and KIP_595_PROTOCOL changes
 */
enum RaftProtocol {
    // kraft support
    KIP_595_PROTOCOL,
    // dynamic quorum reconfiguration support
    KIP_853_PROTOCOL,
    // preVote support
    KIP_996_PROTOCOL;

    boolean isKRaftSupported() {
        return isAtLeast(KIP_595_PROTOCOL);
    }

    boolean isReconfigSupported() {
        return isAtLeast(KIP_853_PROTOCOL);
    }

    boolean isPreVoteSupported() {
        return isAtLeast(KIP_996_PROTOCOL);
    }

    private boolean isAtLeast(RaftProtocol otherRpc) {
        return this.compareTo(otherRpc) >= 0;
    }
}
