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

package org.apache.kafka.castle.role;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.kafka.castle.action.Action;
import org.apache.kafka.castle.cloud.Cloud;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A role which a particular castle cluster node can have.
 *
 * A cluster node may have multiple roles.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = AwsNodeRole.class, name = "awsNode"),
    @JsonSubTypes.Type(value = BrokerRole.class, name = "broker"),
    @JsonSubTypes.Type(value = TrogdorAgentRole.class, name = "trogdorAgent"),
    @JsonSubTypes.Type(value = TrogdorCoordinatorRole.class, name = "trogdorCoordinator"),
    @JsonSubTypes.Type(value = TaskRole.class, name = "task"),
    @JsonSubTypes.Type(value = UbuntuNodeRole.class, name = "ubuntuNode"),
    @JsonSubTypes.Type(value = ZooKeeperRole.class, name = "zooKeeper"),
    @JsonSubTypes.Type(value = CollectdRole.class, name = "collectd"),
    @JsonSubTypes.Type(value = JmxDumperRole.class, name = "jmx"),
    })
public interface Role {
    /**
     * Get the initial delay in milliseconds to use.
     */
    int initialDelayMs();

    /**
     * Create the actions for this node.
     *
     * @param nodeName      The name of this node.
     */
    Collection<Action> createActions(String nodeName);

    /**
     * Get or create the cloud accessor associated with this role, or null if there is none.
     *
     * @param cloudCache    A cache mapping cloud instance names to cloud objects.
     */
    default Cloud cloud(ConcurrentHashMap<String, Cloud> cloudCache) {
        return null;
    }
};
