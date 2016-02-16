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

import java.util.NoSuchElementException;

/**
 * The timestamp type of the records.
 */
public enum TimestampType {
    NO_TIMESTAMP_TYPE(-1, "NoTimestampType"), CREATE_TIME(0, "CreateTime"), LOG_APPEND_TIME(1, "LogAppendTime");

    public final int value;
    public final String name;
    TimestampType(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public static TimestampType getTimestampType(byte attributes) {
        int timestampType = (attributes & Record.TIMESTAMP_TYPE_MASK) >> Record.TIMESTAMP_TYPE_ATTRIBUTE_OFFSET;
        return timestampType == 0 ? CREATE_TIME : LOG_APPEND_TIME;
    }

    public static byte setTimestampType(byte attributes, TimestampType timestampType) {
        return timestampType == CREATE_TIME ?
                (byte) (attributes & ~Record.TIMESTAMP_TYPE_MASK) : (byte) (attributes | Record.TIMESTAMP_TYPE_MASK);
    }

    public static TimestampType forName(String name) {
        switch (name) {
            case "NoTimestampType":
                return NO_TIMESTAMP_TYPE;
            case "CreateTime":
                return CREATE_TIME;
            case "LogAppendTime":
                return LOG_APPEND_TIME;
            default:
                throw new NoSuchElementException("Invalid timestamp type " + name);
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
