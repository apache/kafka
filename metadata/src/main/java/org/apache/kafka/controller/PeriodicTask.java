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

import java.util.EnumSet;
import java.util.function.Supplier;

class PeriodicTask {
    /**
     * The name of this periodic task.
     */
    private final String name;

    /**
     * The callback for this task. If ControllerResult.response is true, we will schedule the
     * task again after only a very short delay. This is useful if we only finished part of the
     * work we wanted to finish.
     */
    private final Supplier<ControllerResult<Boolean>> op;

    /**
     * The period of the task, in nanoseconds.
     */
    private final long periodNs;

    /**
     * The flags used by this periodic task.
     */
    private final EnumSet<PeriodicTaskFlag> flags;

    PeriodicTask(
        String name,
        Supplier<ControllerResult<Boolean>> op,
        long periodNs,
        EnumSet<PeriodicTaskFlag> flags
    ) {
        this.name = name;
        this.op = op;
        this.periodNs = periodNs;
        this.flags = flags;
    }

    String name() {
        return name;
    }

    Supplier<ControllerResult<Boolean>> op() {
        return op;
    }

    long periodNs() {
        return periodNs;
    }

    EnumSet<PeriodicTaskFlag> flags() {
        return flags;
    }
}
