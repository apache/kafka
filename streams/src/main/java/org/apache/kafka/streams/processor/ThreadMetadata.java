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
package org.apache.kafka.streams.processor;

import org.apache.kafka.streams.KafkaStreams;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the state of a single thread running within a {@link KafkaStreams} application.
 */
public class ThreadMetadata {

    private final String threadName;

    private final String threadState;

    private final Set<TaskMetadata> activeTasks;

    private final Set<TaskMetadata> standbyTasks;

    public ThreadMetadata(final String threadName,
                          final String threadState,
                          final Set<TaskMetadata> activeTasks,
                          final Set<TaskMetadata> standbyTasks) {
        this.threadName = threadName;
        this.threadState = threadState;
        this.activeTasks = Collections.unmodifiableSet(activeTasks);
        this.standbyTasks = Collections.unmodifiableSet(standbyTasks);
    }

    public String threadState() {
        return threadState;
    }

    public String threadName() {
        return threadName;
    }

    public Set<TaskMetadata> activeTasks() {
        return activeTasks;
    }

    public Set<TaskMetadata> standbyTasks() {
        return standbyTasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ThreadMetadata that = (ThreadMetadata) o;
        return Objects.equals(threadName, that.threadName) &&
               Objects.equals(threadState, that.threadState) &&
               Objects.equals(activeTasks, that.activeTasks) &&
               Objects.equals(standbyTasks, that.standbyTasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(threadName, threadState, activeTasks, standbyTasks);
    }

    @Override
    public String toString() {
        return "ThreadMetadata{" +
                "threadName=" + threadName +
                ", threadState=" + threadState +
                ", activeTasks=" + activeTasks +
                ", standbyTasks=" + standbyTasks +
                '}';
    }
}
