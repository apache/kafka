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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.Utils;

public class PartitionAssignorAdapter implements PartitionAssignor {

    final org.apache.kafka.clients.consumer.internals.PartitionAssignor oldAssignor;

    PartitionAssignorAdapter(final org.apache.kafka.clients.consumer.internals.PartitionAssignor oldAssignor) {
        this.oldAssignor = oldAssignor;
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return oldAssignor.subscription(topics).userData();
    }

    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription subscriptions) {
        return oldToNewGroupAssignment(oldAssignor.assign(metadata, newToOldGroupSubscription(subscriptions)));
    }

    @Override
    public void onAssignment(Assignment assignment) {
        oldAssignor.onAssignment(newToOldAssignment(assignment));
    }

    @Override
    public void onAssignment(Assignment assignment, int generation) {
        oldAssignor.onAssignment(newToOldAssignment(assignment), generation);
    }

    @Override
    public String name() {
        return oldAssignor.name();
    }

    private org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment newToOldAssignment(Assignment assignment) {
        return new org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment(
            assignment.consumerData().partitions(),assignment.userData());
    }

    private Map<String, org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription> newToOldGroupSubscription(GroupSubscription subscriptions) {
        Map<String, org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription> oldSubscriptions = new HashMap<>();
        for (Map.Entry<String, Subscription> entry : subscriptions.groupSubscription().entrySet()) {
            String member = entry.getKey();
            Subscription newSubscription = entry.getValue();
            oldSubscriptions.put(member, new org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription(
                        newSubscription.consumerData().topics(), newSubscription.userData()));
        }
        return oldSubscriptions;
    }

    private GroupAssignment oldToNewGroupAssignment(Map<String, org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment> assignments) {
        Map<String, Assignment> newAssignments = new HashMap<>();
        for (Map.Entry<String, org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment> entry : assignments.entrySet()) {
            String member = entry.getKey();
            org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment oldAssignment = entry.getValue();
            newAssignments.put(member, new Assignment(new ConsumerAssignmentData(oldAssignment.partitions()), oldAssignment.userData()));
        }
        return new GroupAssignment(newAssignments);
    }

    public static List<PartitionAssignor> getAssignorInstances(AbstractConfig config, String key) {
        List<PartitionAssignor> assignorInstances = new ArrayList<>();
        Class<PartitionAssignor> newClass = PartitionAssignor.class;
        Class<org.apache.kafka.clients.consumer.internals.PartitionAssignor> oldClass = org.apache.kafka.clients.consumer.internals.PartitionAssignor.class;

        List<String> classNames = config.getList(key);
        if (classNames == null)
            return assignorInstances;

        for (Object klass : classNames) {
            Object assignor;
            if (klass instanceof String) {
                try {
                    assignor = Utils.newInstance((String) klass, newClass);
                } catch (ClassNotFoundException e) {
                    throw new KafkaException(klass + " ClassNotFoundException exception occurred", e);
                }
            } else if (klass instanceof Class<?>) {
                assignor = Utils.newInstance((Class<?>) klass);
            } else {
                throw new KafkaException("List contains element of type " + klass.getClass().getName() + ", expected String or Class");
            }

            if (newClass.isInstance(assignor)) {
                assignorInstances.add(newClass.cast(assignor));
            } else if (oldClass.isInstance(assignor)) {
                assignorInstances.add(new PartitionAssignorAdapter(oldClass.cast(assignor)));
            } else {
                throw new KafkaException(klass + " is not an instance of " + oldClass.getName()
                + " or an instance of " + newClass.getName());
            }
        }
        return assignorInstances;
    }

}
