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

/**
 * A listing of a topic in the cluster.
 */
public class TopicListing {
    private final String name;
    private final boolean internal;

    /**
     * Create an instance with the specified parameters.
     *
     * @param name The topic name
     * @param internal Whether the topic is internal to Kafka
     */
    public TopicListing(String name, boolean internal) {
        this.name = name;
        this.internal = internal;
    }

    /**
     * The name of the topic.
     */
    public String name() {
        return name;
    }

    /**
     * Whether the topic is internal to Kafka. An example of an internal topic is the offsets and group management topic:
     * __consumer_offsets.
     */
    public boolean isInternal() {
        return internal;
    }

    @Override
    public String toString() {
        return "(name=" + name + ", internal=" + internal + ")";
    }
}
