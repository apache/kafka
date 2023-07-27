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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Options for {@link AdminClient#removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)}.
 * It carries the members to be removed from the consumer group.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class RemoveMembersFromConsumerGroupOptions extends AbstractOptions<RemoveMembersFromConsumerGroupOptions> {

    private Set<MemberToRemove> members;
    private String reason;

    public RemoveMembersFromConsumerGroupOptions(Collection<MemberToRemove> members) {
        if (members.isEmpty()) {
            throw new IllegalArgumentException("Invalid empty members has been provided");
        }
        this.members = new HashSet<>(members);
    }

    public RemoveMembersFromConsumerGroupOptions() {
        this.members = Collections.emptySet();
    }

    /**
     * Sets an optional reason.
     */
    public void reason(final String reason) {
        this.reason = reason;
    }

    public Set<MemberToRemove> members() {
        return members;
    }

    public String reason() {
        return reason;
    }

    public boolean removeAll() {
        return members.isEmpty();
    }
}
