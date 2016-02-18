/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.errors;

/**
 * Indicates a run time error incurred while trying to assign stream tasks to threads
 */
public class TaskAssignmentException extends StreamsException {

    private final static long serialVersionUID = 1L;

    public TaskAssignmentException(String s) {
        super(s);
    }

    public TaskAssignmentException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public TaskAssignmentException(Throwable throwable) {
        super(throwable);
    }
}
