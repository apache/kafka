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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;
import java.util.Set;

/**
 * The result of the {@link Admin#removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
public class RemoveMembersFromConsumerGroupResult {

    private final KafkaFuture<Map<MemberIdentity, Errors>> future;
    private final Set<MemberToRemove> memberInfos;

    RemoveMembersFromConsumerGroupResult(KafkaFuture<Map<MemberIdentity, Errors>> future,
                                         Set<MemberToRemove> memberInfos) {
        this.future = future;
        this.memberInfos = memberInfos;
    }

    /**
     * Returns a future which indicates whether the request was 100% success, i.e. no
     * either top level or member level error.
     * If not, the first member error shall be returned.
     */
    public KafkaFuture<Void> all() {
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
        this.future.whenComplete((memberErrors, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                if (removeAll()) {
                    for (Map.Entry<MemberIdentity, Errors> entry: memberErrors.entrySet()) {
                        Exception exception = entry.getValue().exception();
                        if (exception != null) {
                            Throwable ex = new KafkaException("Encounter exception when trying to remove: "
                                    + entry.getKey(), exception);
                            result.completeExceptionally(ex);
                            return;
                        }
                    }
                } else {
                    for (MemberToRemove memberToRemove : memberInfos) {
                        if (maybeCompleteExceptionally(memberErrors, memberToRemove.toMemberIdentity(), result)) {
                            return;
                        }
                    }
                }
                result.complete(null);
            }
        });
        return result;
    }

    /**
     * Returns the selected member future.
     */
    public KafkaFuture<Void> memberResult(MemberToRemove member) {
        if (removeAll()) {
            throw new IllegalArgumentException("The method: memberResult is not applicable in 'removeAll' mode");
        }
        if (!memberInfos.contains(member)) {
            throw new IllegalArgumentException("Member " + member + " was not included in the original request");
        }

        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
        this.future.whenComplete((memberErrors, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else if (!maybeCompleteExceptionally(memberErrors, member.toMemberIdentity(), result)) {
                result.complete(null);
            }
        });
        return result;
    }

    private boolean maybeCompleteExceptionally(Map<MemberIdentity, Errors> memberErrors,
                                               MemberIdentity member,
                                               KafkaFutureImpl<Void> result) {
        Throwable exception = KafkaAdminClient.getSubLevelError(memberErrors, member,
            "Member \"" + member + "\" was not included in the removal response");
        if (exception != null) {
            result.completeExceptionally(exception);
            return true;
        } else {
            return false;
        }
    }

    private boolean removeAll() {
        return memberInfos.isEmpty();
    }
}
