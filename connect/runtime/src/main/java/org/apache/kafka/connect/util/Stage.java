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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stage {

    private static final Logger log = LoggerFactory.getLogger(Stage.class);

    private final String description;
    private final long started;
    private volatile Long completed;

    public Stage(String description, long started) {
        this.description = description;
        this.started = started;
        this.completed = null;
    }

    public String description() {
        return description;
    }

    public long started() {
        return started;
    }

    public Long completed() {
        return completed;
    }

    public synchronized void complete(long time) {
        if (time < started) {
            log.warn("Ignoring invalid completion time {} since it is before this stage's start time of {}", time, started);
            return;
        }

        if (completed != null) {
            log.warn("Ignoring completion time of {} since this stage was already completed at {}", time, completed);
            return;
        }

        this.completed = time;
    }

    @Override
    public String toString() {
        return description + "(started " + started + ", completed=" + completed() + ")";
    }

}
