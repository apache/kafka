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

package org.apache.kafka.server.policy;

import java.util.Set;

/** The current state of the topics in the cluster, before the request takes effect. */
public interface ClusterState {
    /**
     * Returns the current state of the given topic, or null if the topic does not exist.
     */
    TopicState topicState(String topicName);

    /**
     * Returns all the topics in the cluster, including internal topics if
     * {@code includeInternal} is true, and including those marked for deletion
     * if {@code includeMarkedForDeletion} is true.
     */
    Set<String> topics(boolean includeInternal, boolean includeMarkedForDeletion);

    /**
     * The number of brokers in the cluster.
     */
    int clusterSize();
}
