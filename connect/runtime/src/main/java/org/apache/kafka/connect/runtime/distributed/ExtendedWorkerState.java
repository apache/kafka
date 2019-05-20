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
package org.apache.kafka.connect.runtime.distributed;

/**
 * A class that captures the deserialized form of a worker's metadata.
 */
public class ExtendedWorkerState extends ConnectProtocol.WorkerState {
    private final ExtendedAssignment assignment;

    public ExtendedWorkerState(String url, long offset, ExtendedAssignment assignment) {
        super(url, offset);
        this.assignment = assignment != null ? assignment : ExtendedAssignment.empty();
    }

    /**
     * This method returns which was the assignment of connectors and tasks on a worker at the
     * moment that its state was captured by this class.
     *
     * @return the assignment of connectors and tasks
     */
    public ExtendedAssignment assignment() {
        return assignment;
    }

    @Override
    public String toString() {
        return "WorkerState{" +
                "url='" + url() + '\'' +
                ", offset=" + offset() +
                ", " + assignment +
                '}';
    }
}
