/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

/**
 * An offset and record pair
 */
public final class LogEntry {

    private final long offset;
    private final Record record;

    public LogEntry(long offset, Record record) {
        this.offset = offset;
        this.record = record;
    }

    public long offset() {
        return this.offset;
    }

    public Record record() {
        return this.record;
    }

    @Override
    public String toString() {
        return "LogEntry(" + offset + ", " + record + ")";
    }
    
    public int size() {
        return record.size() + Records.LOG_OVERHEAD;
    }
}
