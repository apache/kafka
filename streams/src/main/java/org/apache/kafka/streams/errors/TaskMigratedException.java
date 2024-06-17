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
package org.apache.kafka.streams.errors;


/**
 * Indicates that all tasks belongs to the thread have migrated to another thread. This exception can be thrown when
 * the thread gets fenced (either by the consumer coordinator or by the transaction coordinator), which means it is
 * no longer part of the group but a "zombie" already
 */
public class TaskMigratedException extends StreamsException {

    private final static long serialVersionUID = 1L;

    public TaskMigratedException(final String message) {
        super(message + "; it means all tasks belonging to this thread should be migrated.");
    }

    public TaskMigratedException(final String message, final Throwable throwable) {
        super(message + "; it means all tasks belonging to this thread should be migrated.", throwable);
    }
}
