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
package org.apache.kafka.server.util.timer;

public interface Timer extends AutoCloseable {
    /**
     * Add a new task to this executor. It will be executed after the task's delay
     * (beginning from the time of submission)
     * @param timerTask the task to add
     */
    void add(TimerTask timerTask);

    /**
     * Advance the internal clock, executing any tasks whose expiration has been
     * reached within the duration of the passed timeout.
     * @param timeoutMs the time to advance in milliseconds
     * @return whether or not any tasks were executed
     */
    boolean advanceClock(long timeoutMs) throws InterruptedException;

    /**
     * Get the number of tasks pending execution
     * @return the number of tasks
     */
    int size();

}
