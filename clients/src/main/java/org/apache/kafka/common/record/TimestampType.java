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
 * The timestamp type of the records.
 */
public enum TimestampType {
    CreateTime(0, "CreateTime"), LogAppendTime(1, "LogAppendTime");

    public int value;
    public String name;
    TimestampType(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public static TimestampType getTimestampType(byte attributes) {
        int timestampType = (attributes & Record.TIMESTAMP_TYPE_MASK) >> Record.TIMESTAMP_TYPE_ATTRIBUTE_OFFSET;
        return timestampType == 0 ? CreateTime : LogAppendTime;
    }

    public static byte setTimestampType(byte attributes, TimestampType timestampType) {
        return timestampType == CreateTime ?
                (byte) (attributes & ~Record.TIMESTAMP_TYPE_MASK) : (byte) (attributes | Record.TIMESTAMP_TYPE_MASK);
    }
}
