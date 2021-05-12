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

package org.apache.kafka.controller;

public final class MockControllerMetrics implements ControllerMetrics {
    private volatile boolean active;
    private volatile int topics;
    private volatile int partitions;

    public MockControllerMetrics() {
        this.active = false;
        this.topics = 0;
        this.partitions = 0;
    }

    @Override
    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public boolean active() {
        return this.active;
    }

    @Override
    public void updateEventQueueTime(long durationMs) {
        // nothing to do
    }

    @Override
    public void updateEventQueueProcessingTime(long durationMs) {
        // nothing to do
    }

    @Override
    public void setGlobalTopicsCount(int topicCount) {
        this.topics = topicCount;
    }

    @Override
    public int globalTopicsCount() {
        return this.topics;
    }

    @Override
    public void setGlobalPartitionCount(int partitionCount) {
        this.partitions = partitionCount;
    }

    @Override
    public int globalPartitionCount() {
        return this.partitions;
    }
}
