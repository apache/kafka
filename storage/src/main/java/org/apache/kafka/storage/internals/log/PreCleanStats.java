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

/**
 * A simple struct for collecting pre-clean stats.
 */
public class PreCleanStats {
    private long maxCompactionDelayMs = 0L;
    private int delayedPartitions = 0;
    private int cleanablePartitions = 0;

    public void updateMaxCompactionDelay(long delayMs) {
        maxCompactionDelayMs = Math.max(maxCompactionDelayMs, delayMs);
        if (delayMs > 0) {
            delayedPartitions++;
        }
    }

    public void recordCleanablePartitions(int numOfCleanables) {
        cleanablePartitions = numOfCleanables;
    }

    public int cleanablePartitions() {
        return cleanablePartitions;
    }

    public int delayedPartitions() {
        return delayedPartitions;
    }

    public long maxCompactionDelayMs() {
        return maxCompactionDelayMs;
    }

    // for testing
    public void maxCompactionDelayMs(long maxCompactionDelayMs) {
        this.maxCompactionDelayMs = maxCompactionDelayMs;
    }
}
