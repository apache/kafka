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
package org.apache.kafka.coordinator.group.api.streams.assignor;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;

/**
 * The assignment configurations that the group coordinator passes to the task assignor.
 *
 * <p>This interface is not intended to be implemented by task assignors: new configurations may be added to it.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AssignmentConfigs {

    /**
     * @return The number of standby replicas for each task.
     */
    int numStandbyReplicas();

    /**
     * @return The client tags used to distribute standby tasks across racks. The list is unmodifiable.
     */
    List<String> rackAwareAssignmentTags();

}
