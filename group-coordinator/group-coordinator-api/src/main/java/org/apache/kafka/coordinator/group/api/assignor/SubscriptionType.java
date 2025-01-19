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
package org.apache.kafka.coordinator.group.api.assignor;

/**
 * The subscription type followed by a consumer group.
 */
public enum SubscriptionType {
    /**
     * A homogeneous subscription type means that all the members
     * of the group use the same subscription.
     */
    HOMOGENEOUS("Homogeneous"),
    /**
     * A heterogeneous subscription type means that not all the members
     * of the group use the same subscription.
     */
    HETEROGENEOUS("Heterogeneous");

    private final String name;

    SubscriptionType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
