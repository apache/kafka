/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.transforms.util;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public class Requirements {

    public static void requireSchema(Schema schema, String purpose) {
        if (schema == null) {
            throw new DataException("Schema required for [" + purpose + "]");
        }
    }

    public static Map<String, Object> requireMap(Object value, String purpose) {
        if (!(value instanceof Map)) {
            throw new DataException("Only Map objects supported in absence of schema for [" + purpose + "], found: " + nullSafeClassName(value));
        }
        return (Map<String, Object>) value;
    }

    public static Struct requireStruct(Object value, String purpose) {
        if (!(value instanceof Struct)) {
            throw new DataException("Only Struct objects supported for [" + purpose + "], found: " + nullSafeClassName(value));
        }
        return (Struct) value;
    }

    public static SinkRecord requireSinkRecord(ConnectRecord<?> record, String purpose) {
        if (!(record instanceof SinkRecord)) {
            throw new DataException("Only SinkRecord supported for [" + purpose + "], found: " + nullSafeClassName(record));
        }
        return (SinkRecord) record;
    }

    private static String nullSafeClassName(Object x) {
        return x == null ? "null" : x.getClass().getCanonicalName();
    }

}
