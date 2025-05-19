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
package org.apache.kafka.storage.internals.log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class LogManager {

    public static final String LOCK_FILE_NAME = ".lock";
    public static final String RECOVERY_POINT_CHECKPOINT_FILE = "recovery-point-offset-checkpoint";
    public static final String LOG_START_OFFSET_CHECKPOINT_FILE = "log-start-offset-checkpoint";

    /**
     * Wait for all jobs to complete
     * @param jobs The jobs
     * @param callback This will be called to handle the exception caused by each Future#get
     * @return true if all pass. Otherwise, false
     */
    public static boolean waitForAllToComplete(List<Future<?>> jobs, Consumer<Throwable> callback) {
        List<Future<?>> failed = new ArrayList<>();
        for (Future<?> job : jobs) {
            try {
                job.get();
            } catch (Exception e) {
                callback.accept(e);
                failed.add(job);
            }
        }
        return failed.isEmpty();
    }
}
