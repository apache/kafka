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

import org.apache.kafka.controller.errors.PeriodicControlTaskException;

import java.util.EnumSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class PeriodicTask {
    /**
     * The name of this periodic task.
     */
    private final String name;

    /**
     * The write operation for this periodic task. It contains two callbacks, one for generating records
     * and a controller result, and one for processing the end offset of the batch.
     * If ControllerResult.response is true, we will schedule the task again after only a very short delay.
     * This is useful if we only finished part of the work we wanted to finish.
     */
    private final QuorumController.ControllerWriteOperation<Boolean> writeOp;

    /**
     * The period of the task when ControllerResult.response is true, in nanoseconds.
     */
    private final long immediatePeriodNs;

    /**
     * The default period of the task when ControllerResult.response is false, in nanoseconds.
     */
    private final long periodNs;

    /**
     * The flags used by this periodic task.
     */
    private final EnumSet<PeriodicTaskFlag> flags;

    private static final long DEFAULT_IMMEDIATE_PERIOD_NS = MILLISECONDS.toNanos(10);

    PeriodicTask(
        String name,
        QuorumController.ControllerWriteOperation<Boolean> writeOp,
        long periodNs,
        EnumSet<PeriodicTaskFlag> flags
    ) {
        this(name, writeOp, periodNs, flags, DEFAULT_IMMEDIATE_PERIOD_NS);
    }

    PeriodicTask(
        String name,
        QuorumController.ControllerWriteOperation<Boolean> writeOp,
        long periodNs,
        EnumSet<PeriodicTaskFlag> flags,
        long immediatePeriodNs
    ) {
        this.name = name;
        this.writeOp = writeOp;
        this.periodNs = periodNs;
        this.flags = flags;
        this.immediatePeriodNs = immediatePeriodNs;
    }

    String name() {
        return name;
    }

    Supplier<ControllerResult<Boolean>> op() {
        return () -> {
            try {
                return writeOp.generateRecordsAndResult();
            } catch (Exception e) {
                throw new PeriodicControlTaskException(name + ": periodic task failed: " +
                    e.getMessage(), e);
            }
        };
    }

    Consumer<Long> processBatchEndOffsetOp() {
        return writeOp::processBatchEndOffset;
    }

    long immediatePeriodNs() {
        return immediatePeriodNs;
    }

    long periodNs() {
        return periodNs;
    }

    EnumSet<PeriodicTaskFlag> flags() {
        return flags;
    }
}
