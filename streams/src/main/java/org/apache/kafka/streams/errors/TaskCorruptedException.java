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

import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Set;

/**
 * Indicates a specific task is corrupted and need to be re-initialized. It can be thrown when
 *
 * 1) Under EOS, if the checkpoint file does not contain offsets for corresponding store's changelogs, meaning
 *    previously it was not close cleanly;
 * 2) Out-of-range exception thrown during restoration, meaning that the changelog has been modified and we re-bootstrap
 *    the store.
 */
public class TaskCorruptedException extends StreamsException {

    private final Set<TaskId> corruptedTasks;

    public TaskCorruptedException(final Set<TaskId> corruptedTasks) {
        super("Tasks " + corruptedTasks + " are corrupted and hence needs to be re-initialized");
        this.corruptedTasks = corruptedTasks;
    }

    public TaskCorruptedException(final Set<TaskId> corruptedTasks,
                                  final InvalidOffsetException e) {
        super("Tasks " + corruptedTasks + " are corrupted and hence needs to be re-initialized", e);
        this.corruptedTasks = corruptedTasks;
    }

    public Set<TaskId> corruptedTasks() {
        return corruptedTasks;
    }
}
