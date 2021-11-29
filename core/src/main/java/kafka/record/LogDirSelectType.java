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
package kafka.record;

import java.util.NoSuchElementException;

/**
 * The log dir selector type of the records.
 */
public enum LogDirSelectType {
    NO_LOG_DIR_SELECT_TYPE(-1, "NoTimestampType"), PARTITION(0, "Partition"), SIZE(1, "Size");

    public final int id;
    public final String name;

    LogDirSelectType(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public static LogDirSelectType forName(String name) {
        for (LogDirSelectType t : values())
            if (t.name.equals(name))
                return t;
        throw new NoSuchElementException("Invalid log dir selector type " + name);
    }

    @Override
    public String toString() {
        return name;
    }
}
