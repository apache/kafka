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
package org.apache.kafka.coordinator.group.assignor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.apache.kafka.coordinator.group.assignor.SubscriptionType.HOMOGENEOUS;

/**
 * The Uniform Assignor distributes topic partitions among group members for a
 * balanced and potentially rack aware assignment.
 * The assignor employs two different strategies based on the nature of topic
 * subscriptions across the group members:
 * <ul>
 *     <li>
 *         <b> Optimized Uniform Assignment Builder: </b> This strategy is used when all members have subscribed
 *         to the same set of topics.
 *     </li>
 *     <li>
 *         <b> General Uniform Assignment Builder: </b> This strategy is used when members have varied topic
 *         subscriptions.
 *     </li>
 * </ul>
 *
 * The appropriate strategy is automatically chosen based on the current members' topic subscriptions.
 *
 * @see OptimizedUniformAssignmentBuilder
 * @see GeneralUniformAssignmentBuilder
 */
public class UniformAssignor implements PartitionAssignor {
    private static final Logger LOG = LoggerFactory.getLogger(UniformAssignor.class);
    public static final String UNIFORM_ASSIGNOR_NAME = "uniform";

    @Override
    public String name() {
        return UNIFORM_ASSIGNOR_NAME;
    }

    /**
     * Perform the group assignment given the current members and
     * topics metadata.
     *
     * @param assignmentSpec                The assignment specification that included member metadata.
     * @param subscribedTopicDescriber      The topic and cluster metadata describer {@link SubscribedTopicDescriber}.
     * @return The new target assignment for the group.
     */
    @Override
    public GroupAssignment assign(
        AssignmentSpec assignmentSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        AbstractUniformAssignmentBuilder assignmentBuilder;

        if (assignmentSpec.members().isEmpty())
            return new GroupAssignment(Collections.emptyMap());

        if (assignmentSpec.subscriptionType().equals(HOMOGENEOUS)) {
            LOG.debug("Detected that all members are subscribed to the same set of topics, invoking the "
                + "optimized assignment algorithm");
            assignmentBuilder = new OptimizedUniformAssignmentBuilder(assignmentSpec, subscribedTopicDescriber);
        } else {
            LOG.debug("Detected that the members are subscribed to different sets of topics, invoking the "
                + "general assignment algorithm");
            assignmentBuilder = new GeneralUniformAssignmentBuilder(assignmentSpec, subscribedTopicDescriber);
        }

        return assignmentBuilder.buildAssignment();
    }
}
