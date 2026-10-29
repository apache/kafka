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

import org.apache.kafka.common.utils.Time;

/**
 * A simple struct for collecting stats about log cleaning.
 */
public class CleanerStats {
    long bytesRead = 0L;
    long bytesWritten = 0L;
    long mapBytesRead = 0L;
    long messagesRead = 0L;
    long invalidMessagesRead = 0L;
    long messagesWritten = 0L;
    double bufferUtilization = 0.0d;

    private long mapCompleteTime = -1L;
    private long endTime = -1L;
    private long mapMessagesRead = 0L;

    private final Time time;
    private final long startTime;

    public CleanerStats(Time time) {
        this.time = time;
        startTime = time.milliseconds();
    }

    public void readMessages(int messagesRead, int bytesRead) {
        this.messagesRead += messagesRead;
        this.bytesRead += bytesRead;
    }

    public void invalidMessage() {
        invalidMessagesRead += 1;
    }

    public void recopyMessages(int messagesWritten, int bytesWritten) {
        this.messagesWritten += messagesWritten;
        this.bytesWritten += bytesWritten;
    }

    public void indexMessagesRead(int size) {
        mapMessagesRead += size;
    }

    public void indexBytesRead(int size) {
        mapBytesRead += size;
    }

    public void indexDone() {
        mapCompleteTime = time.milliseconds();
    }

    public void allDone() {
        endTime = time.milliseconds();
    }

    public double elapsedSecs() {
        return (endTime - startTime) / 1000.0;
    }

    public double elapsedIndexSecs() {
        return (mapCompleteTime - startTime) / 1000.0;
    }

    // Only for testing
    public long startTime() {
        return startTime;
    }

    // Only for testing
    public long endTime() {
        return endTime;
    }

    // Only for testing
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    // Only for testing
    public long bytesRead() {
        return bytesRead;
    }

    // Only for testing
    public long bytesWritten() {
        return bytesWritten;
    }

    // Only for testing
    public long mapMessagesRead() {
        return mapMessagesRead;
    }

    // Only for testing
    public long messagesRead() {
        return messagesRead;
    }

    // Only for testing
    public long invalidMessagesRead() {
        return invalidMessagesRead;
    }

    // Only for testing
    public long messagesWritten() {
        return messagesWritten;
    }

    // Only for testing
    public double bufferUtilization() {
        return bufferUtilization;
    }

    // Only for testing
    public void setBufferUtilization(double bufferUtilization) {
        this.bufferUtilization = bufferUtilization;
    }
}
