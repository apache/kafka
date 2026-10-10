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
package org.apache.kafka.coordinator.group.assignor.uniform2;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

/**
 * The model, the current assignment and the shares of a group, for the tests of the steps
 * deciding the shares. The shares hold the base partitions only, to which a test adds the extra
 * partitions it starts from. The members and topics are referred to by their ids.
 */
final class SharesFixture {
    /**
     * The members and topics of the group.
     */
    final GroupModel group;

    /**
     * The current assignment of the group.
     */
    final CurrentAssignment current;

    /**
     * The shares of the members.
     */
    final Shares shares;

    /**
     * Creates the fixture with the base partitions only.
     *
     * @param groupSpec The group spec.
     * @param describer The describer of the topics.
     */
    SharesFixture(GroupSpec groupSpec, SubscribedTopicDescriber describer) {
        group = new GroupModel(groupSpec, describer);
        current = new CurrentAssignment(groupSpec, group);
        shares = new Shares(group);
    }

    /**
     * @return The number of the member.
     */
    int member(String memberId) {
        for (int member = 0; member < group.memberCount(); member++) {
            if (group.memberId(member).equals(memberId)) {
                return member;
            }
        }
        throw new IllegalArgumentException("Unknown member " + memberId);
    }

    /**
     * @return The number of the topic.
     */
    int topic(Uuid topicId) {
        return group.topicIndex(topicId);
    }

    /**
     * Gives the member an extra partition of every topic, which it must not have yet.
     */
    void addExtras(String memberId, Uuid... topicIds) {
        for (Uuid topicId : topicIds) {
            if (shares.membersWithExtra(topic(topicId)).indexOf(member(memberId)) >= 0) {
                throw new IllegalArgumentException(memberId + " already has an extra partition of " + topicId);
            }
            shares.giveExtraPartition(member(memberId), topic(topicId));
        }
    }

    /**
     * @return Per topic of which some members have an extra partition, these members, in member
     *         id order.
     */
    Map<Uuid, List<String>> membersWithExtra() {
        var result = new HashMap<Uuid, List<String>>();
        for (int topic = 0; topic < group.topicCount(); topic++) {
            var members = shares.membersWithExtra(topic);
            if (!members.isEmpty()) {
                var memberIds = new ArrayList<String>();
                for (int i = 0; i < members.size(); i++) {
                    memberIds.add(group.memberId(members.get(i)));
                }
                memberIds.sort(null);
                result.put(group.topicId(topic), memberIds);
            }
        }
        return result;
    }

    /**
     * @return The members for which the predicate holds, in member id order.
     */
    List<String> members(IntPredicate predicate) {
        var memberIds = new ArrayList<String>();
        for (int member = 0; member < group.memberCount(); member++) {
            if (predicate.test(member)) {
                memberIds.add(group.memberId(member));
            }
        }
        return memberIds;
    }

    /**
     * @return The assignment sizes of the members, in member id order.
     */
    List<Integer> sizes() {
        var sizes = new ArrayList<Integer>();
        for (int member = 0; member < group.memberCount(); member++) {
            sizes.add(shares.size(member));
        }
        return sizes;
    }
}
