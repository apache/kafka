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
package org.apache.kafka.connect.util;

import java.util.concurrent.atomic.AtomicLong;

public class Stage {
    private final String description;
    private final long started;
    private final AtomicLong completed;

    public Stage(String description, long started) {
        if (started < 0)
            throw new IllegalArgumentException("Invalid start timestamp " + started + "; cannot be negative");

        this.description = description;
        this.started = started;
        this.completed = new AtomicLong(-1);
    }

    public String description() {
        return description;
    }

    public long started() {
        return started;
    }

    public Long completed() {
        long result = completed.get();
        return result >= 0 ? result : null;
    }

    public synchronized void complete(long time) {
        if (time < 0)
            throw new IllegalArgumentException("Cannot complete stage with negative timestamp " + time);
        if (time < started)
            throw new IllegalArgumentException("Cannot complete stage with timestamp " + time + " before its start time " + started);

        this.completed.updateAndGet(l -> {
            if (l >= 0)
                throw new IllegalStateException("Stage is already completed");

            return time;
        });
    }

    @Override
    public String toString() {
        return description + "(started " + started + ", completed=" + completed() + ")";
    }

}
