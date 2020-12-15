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
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This adapter class is used to ensure backwards compatibility for those who have implemented the {@link PartitionAssignor}
 * interface, which has been deprecated in favor of the new {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor}.
 * <p>
 * Note that maintaining compatibility for an internal interface here is a special case, as {@code PartitionAssignor}
 * was meant to be a public API although it was placed in the internals package. Users should not expect internal
 * interfaces or classes to not be removed or maintain compatibility in any way.
 */
@SuppressWarnings("deprecation")
public class PartitionAssignorAdapter implements ConsumerPartitionAssignor {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionAssignorAdapter.class);
    private final PartitionAssignor oldAssignor;

    PartitionAssignorAdapter(PartitionAssignor oldAssignor) {
        this.oldAssignor = oldAssignor;
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return oldAssignor.subscription(topics).userData();
    }

    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        return toNewGroupAssignment(oldAssignor.assign(metadata, toOldGroupSubscription(groupSubscription)));
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        oldAssignor.onAssignment(toOldAssignment(assignment), metadata.generationId());
    }

    @Override
    public String name() {
        return oldAssignor.name();
    }

    private static PartitionAssignor.Assignment toOldAssignment(Assignment newAssignment) {
        return new PartitionAssignor.Assignment(newAssignment.partitions(), newAssignment.userData());
    }

    private static Map<String, PartitionAssignor.Subscription> toOldGroupSubscription(GroupSubscription newSubscriptions) {
        Map<String, PartitionAssignor.Subscription> oldSubscriptions = new HashMap<>();
        for (Map.Entry<String, Subscription> entry : newSubscriptions.groupSubscription().entrySet()) {
            String member = entry.getKey();
            Subscription newSubscription = entry.getValue();
            oldSubscriptions.put(member, new PartitionAssignor.Subscription(
                newSubscription.topics(), newSubscription.userData()));
        }
        return oldSubscriptions;
    }

    private static GroupAssignment toNewGroupAssignment(Map<String, PartitionAssignor.Assignment> oldAssignments) {
        Map<String, Assignment> newAssignments = new HashMap<>();
        for (Map.Entry<String, PartitionAssignor.Assignment> entry : oldAssignments.entrySet()) {
            String member = entry.getKey();
            PartitionAssignor.Assignment oldAssignment = entry.getValue();
            newAssignments.put(member, new Assignment(oldAssignment.partitions(), oldAssignment.userData()));
        }
        return new GroupAssignment(newAssignments);
    }

    /**
     * Get a list of configured instances of {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor}
     * based on the class names/types specified by {@link org.apache.kafka.clients.consumer.ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
     * where any instances of the old {@link PartitionAssignor} interface are wrapped in an adapter to the new
     * {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor} interface
     */
    public static List<ConsumerPartitionAssignor> getAssignorInstances(List<String> assignorClasses, Map<String, Object> configs) {
        List<ConsumerPartitionAssignor> assignors = new ArrayList<>();

        if (assignorClasses == null)
            return assignors;

        for (Object klass : assignorClasses) {
            // first try to get the class if passed in as a string
            if (klass instanceof String) {
                try {
                    klass = Class.forName((String) klass, true, Utils.getContextOrKafkaClassLoader());
                } catch (ClassNotFoundException classNotFound) {
                    throw new KafkaException(klass + " ClassNotFoundException exception occurred", classNotFound);
                }
            }

            if (klass instanceof Class<?>) {
                Object assignor = Utils.newInstance((Class<?>) klass);
                if (assignor instanceof Configurable)
                    ((Configurable) assignor).configure(configs);

                if (assignor instanceof ConsumerPartitionAssignor) {
                    assignors.add((ConsumerPartitionAssignor) assignor);
                } else if (assignor instanceof PartitionAssignor) {
                    assignors.add(new PartitionAssignorAdapter((PartitionAssignor) assignor));
                    LOG.warn("The PartitionAssignor interface has been deprecated, "
                        + "please implement the ConsumerPartitionAssignor interface instead.");
                } else {
                    throw new KafkaException(klass + " is not an instance of " + PartitionAssignor.class.getName()
                        + " or an instance of " + ConsumerPartitionAssignor.class.getName());
                }
            } else {
                throw new KafkaException("List contains element of type " + klass.getClass().getName() + ", expected String or Class");
            }
        }
        return assignors;
    }
}
