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

import java.util.Map;

/**
 * Options for {@link AdminClient#alterPartitionReassignments(Map, AlterPartitionReassignmentsOptions)}
 */
public class AlterPartitionReassignmentsOptions extends AbstractOptions<AlterPartitionReassignmentsOptions> {

    private boolean allowReplicationFactorChange = true;

    /**
     * Set the option indicating if the alter partition reassignments call should be
     * allowed to alter the replication factor of a partition.
     * In cases where it is not allowed, any replication factor change will result in an exception thrown by the API.
     */
    public AlterPartitionReassignmentsOptions allowReplicationFactorChange(boolean allow) {
        this.allowReplicationFactorChange = allow;
        return this;
    }

    /**
     * A boolean indicating if the alter partition reassignments should be
     * allowed to alter the replication factor of a partition.
     * In cases where it is not allowed, any replication factor change will result in an exception thrown by the API.
     */
    public boolean allowReplicationFactorChange() {
        return this.allowReplicationFactorChange;
    }
}
