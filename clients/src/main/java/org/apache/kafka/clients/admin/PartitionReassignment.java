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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A partition reassignment, which has been listed via {@link AdminClient#listPartitionReassignments()}.
 */
public class PartitionReassignment {
    /**
     * The brokers which we want this partition to reside on.
     */
    private final List<Integer> targetBrokers;

    /**
     * The brokers which this partition currently resides on.
     */
    private final List<Integer> currentBrokers;

    public PartitionReassignment(List<Integer> targetBrokers,
                                 List<Integer> currentBrokers) {
        this.targetBrokers = Collections.unmodifiableList(new ArrayList<>(targetBrokers));
        this.currentBrokers = Collections.unmodifiableList(new ArrayList<>(currentBrokers));
    }

    public List<Integer> targetBrokers() {
        return targetBrokers;
    }

    public List<Integer> currentBrokers() {
        return currentBrokers;
    }
}
