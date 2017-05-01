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

import org.apache.kafka.common.utils.Utils;

import java.util.NavigableMap;

/**
 * A detailed description of a single topic in the cluster.
 */
public class TopicDescription {
    private final String name;
    private final boolean internal;
    private final NavigableMap<Integer, TopicPartitionInfo> partitions;

    TopicDescription(String name, boolean internal,
                    NavigableMap<Integer, TopicPartitionInfo> partitions) {
        this.name = name;
        this.internal = internal;
        this.partitions = partitions;
    }

    public String name() {
        return name;
    }

    public boolean internal() {
        return internal;
    }

    public NavigableMap<Integer, TopicPartitionInfo> partitions() {
        return partitions;
    }

    @Override
    public String toString() {
        return "(name=" + name + ", internal=" + internal + ", partitions=" +
            Utils.mkString(partitions, "[", "]", "=", ",") + ")";
    }
}
